// mobile/store/usePropsStore.ts
import { create } from "zustand";
import { MobileProp } from "@/lib/api";

type PropsStore = {
  props: MobileProp[];
  setProps: (props: MobileProp[]) => void;
};

export const usePropsStore = create<PropsStore>((set) => ({
  props: [],
  setProps: (props) => set({ props }),
}));