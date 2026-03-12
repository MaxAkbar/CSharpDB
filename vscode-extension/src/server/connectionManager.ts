import * as vscode from "vscode";
import { CSharpDbApiClient } from "../api/client";
import { DatabaseInfoResponse } from "../api/types";

export interface ConnectOptions {
  databasePath?: string;
}

export interface ConnectionState {
  connected: boolean;
  connecting: boolean;
  databasePath?: string;
  nativeLibraryPath?: string;
  resolvedNativeLibraryPath?: string;
  info?: DatabaseInfoResponse;
  lastError?: string;
  autoStarted: boolean;
}

export class ConnectionManager implements vscode.Disposable {
  private readonly emitter = new vscode.EventEmitter<ConnectionState>();
  private state: ConnectionState;

  readonly onConnectionChanged = this.emitter.event;

  constructor(
    private readonly client: CSharpDbApiClient,
    private readonly output: vscode.OutputChannel
  ) {
    this.state = {
      connected: false,
      connecting: false,
      nativeLibraryPath: client.getNativeLibraryPath(),
      resolvedNativeLibraryPath: client.getResolvedNativeLibraryPath(),
      autoStarted: false
    };
  }

  get currentState(): ConnectionState {
    return this.state;
  }

  async connect(options: ConnectOptions = {}): Promise<ConnectionState> {
    const databasePath = options.databasePath ?? this.state.databasePath;
    if (!databasePath) {
      throw new Error("No database file was selected.");
    }

    const nativeLibraryPath = vscode.workspace.getConfiguration("csharpdb").get<string>("nativeLibraryPath", "").trim();
    this.client.setNativeLibraryPath(nativeLibraryPath);
    this.updateState({
      ...this.state,
      connected: false,
      connecting: true,
      databasePath,
      nativeLibraryPath,
      lastError: undefined
    });

    try {
      const info = await this.client.open(databasePath);
      this.updateState({
        connected: true,
        connecting: false,
        databasePath,
        nativeLibraryPath,
        resolvedNativeLibraryPath: this.client.getResolvedNativeLibraryPath(),
        info,
        autoStarted: false,
        lastError: undefined
      });

      return this.state;
    } catch (error) {
      this.updateState({
        connected: false,
        connecting: false,
        databasePath,
        nativeLibraryPath,
        resolvedNativeLibraryPath: this.client.getResolvedNativeLibraryPath(),
        info: undefined,
        autoStarted: false,
        lastError: toErrorMessage(error)
      });

      throw error;
    }
  }

  async ensureConnected(options: ConnectOptions = {}): Promise<ConnectionState> {
    if (this.state.connected) {
      return this.state;
    }

    return this.connect(options);
  }

  async refresh(): Promise<ConnectionState> {
    if (!this.state.connected || this.state.connecting) {
      return this.state;
    }

    try {
      const info = await this.client.getDatabaseInfo();
      this.updateState({
        ...this.state,
        connected: true,
        connecting: false,
        resolvedNativeLibraryPath: this.client.getResolvedNativeLibraryPath(),
        info,
        lastError: undefined
      });
    } catch (error) {
      const message = toErrorMessage(error);
      this.output.appendLine(`[connection] ${message}`);
      this.updateState({
        ...this.state,
        connected: false,
        connecting: false,
        info: undefined,
        lastError: message
      });
    }

    return this.state;
  }

  async disconnect(): Promise<void> {
    await this.client.close();
    this.updateState({
      connected: false,
      connecting: false,
      databasePath: this.state.databasePath,
      nativeLibraryPath: this.client.getNativeLibraryPath(),
      resolvedNativeLibraryPath: this.client.getResolvedNativeLibraryPath(),
      info: undefined,
      autoStarted: false,
      lastError: undefined
    });
  }

  dispose(): void {
    void this.client.close();
    this.emitter.dispose();
  }

  private updateState(nextState: ConnectionState): void {
    this.state = nextState;
    this.emitter.fire(this.state);
  }
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
