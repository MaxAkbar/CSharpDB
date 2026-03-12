import { ProcedureParameterResponse } from "./types";
import { asString, requireNonEmpty } from "./sqlHelpers";

interface ProcedureParameterStorage {
  name?: string;
  type?: string;
  required?: boolean;
  default?: unknown;
  description?: string | null;
}

export function parseProcedureParameters(value: unknown): ProcedureParameterStorage[] {
  const raw = asString(value);
  if (!raw || raw.trim().length === 0) {
    return [];
  }

  try {
    const parsed = JSON.parse(raw) as ProcedureParameterStorage[];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export function normalizeProcedureParameters(
  parameters: Array<ProcedureParameterStorage | ProcedureParameterResponse>
): ProcedureParameterResponse[] {
  return parameters
    .filter((parameter): parameter is ProcedureParameterStorage | ProcedureParameterResponse => Boolean(parameter))
    .map((parameter) => ({
      name: requireNonEmpty(parameter.name ?? "", "Procedure parameter name is required."),
      type: requireNonEmpty(parameter.type ?? "TEXT", "Procedure parameter type is required.").toUpperCase(),
      required: Boolean(parameter.required),
      default: parameter.default,
      description: parameter.description ?? null
    }));
}

export function serializeProcedureParameters(
  parameters: Array<ProcedureParameterStorage | ProcedureParameterResponse>
): string {
  return JSON.stringify(normalizeProcedureParameters(parameters));
}
