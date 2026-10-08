// Copyright (c) 2026 Kenneth Stott
// Canary: 0d7cff8c-9de3-4989-bcd1-142eac447114
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** The data product form's state and its empty value (REQ-1634). */

export interface DataProductForm {
  id: string;
  domainId: string;
  name: string;
  ownerRole: string;
  teamRole: string;
  purpose: string;
  limitations: string;
  usage: string;
  version: string;
  status: string;
  sla: string;
  support: string;
  customProperties: Record<string, string>;
}

export const EMPTY_FORM: DataProductForm = {
  id: "",
  domainId: "",
  name: "",
  ownerRole: "",
  teamRole: "",
  purpose: "",
  limitations: "",
  usage: "",
  version: "",
  status: "",
  sla: "",
  support: "",
  customProperties: {},
};
