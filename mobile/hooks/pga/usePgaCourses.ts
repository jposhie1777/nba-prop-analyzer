import { usePgaQuery } from "./usePgaQuery";
import { PgaCourse } from "@/types/pga";

type Response = {
  data: PgaCourse[];
  count: number;
};

export function usePgaCourses(params?: { search?: string }) {
  return usePgaQuery<Response>("/pga/courses", {
    search: params?.search,
    per_page: 100,
  });
}
