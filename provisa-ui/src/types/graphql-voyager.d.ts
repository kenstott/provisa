// Copyright (c) 2026 Kenneth Stott
// Canary: 35de07e0-5709-41ec-9361-b1eb0cbf5d61
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

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
