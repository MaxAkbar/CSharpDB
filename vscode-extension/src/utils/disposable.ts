import * as vscode from "vscode";

export class DisposableStore implements vscode.Disposable {
  private readonly disposables: vscode.Disposable[] = [];

  add<T extends vscode.Disposable>(value: T): T {
    this.disposables.push(value);
    return value;
  }

  dispose(): void {
    while (this.disposables.length > 0) {
      this.disposables.pop()?.dispose();
    }
  }
}
