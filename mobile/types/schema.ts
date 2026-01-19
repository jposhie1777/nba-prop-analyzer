// types/schema.ts
export type ColumnSchema = {
  name: string;
  type: "STRING" | "INTEGER" | "FLOAT";
};

export type ColumnConfig = {
  key: string;
  label: string;
  width: number;
  isNumeric: boolean;
  formatter?: (v: any) => string;
};