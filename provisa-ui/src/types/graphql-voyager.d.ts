declare module "graphql-voyager" {
  import { ComponentType } from "react";

  interface VoyagerProps {
    introspection: (query: string) => any;
    displayOptions?: {
      skipRelay?: boolean;
      skipDeprecated?: boolean;
      rootType?: string;
      sortByAlphabet?: boolean;
      showLeafFields?: boolean;
      hideRoot?: boolean;
    };
  }

  export const Voyager: ComponentType<VoyagerProps>;
}
