import "vitest";

declare module "vitest" {
  export interface ProvidedContext {
    readonly ordersDemoWsUrl: string;
  }
}

declare module "vite-plus/test" {
  export interface ProvidedContext {
    readonly ordersDemoWsUrl: string;
  }
}
