import "vite-plus/test";
import "vitest";

declare module "vitest" {
  export interface ProvidedContext {
    readonly viewServerWsUrl: string;
  }
}

declare module "vite-plus/test" {
  export interface ProvidedContext {
    readonly viewServerWsUrl: string;
  }
}
