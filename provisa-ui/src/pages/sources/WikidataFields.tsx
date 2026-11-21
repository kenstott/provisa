// Copyright (c) 2026 Kenneth Stott
// Canary: 2d8f4a63-c17e-4b92-a5d0-7e3b9c1f6a28
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: a Wikidata source asks for nothing: its endpoint is the query service, shown, and it
// takes no credential.

import { TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";
import { WIKIDATA_ENDPOINT } from "./wikidata";

export function WikidataFields() {
  const { t } = useTranslation();
  return (
    <FormField hint={t("sourceFormFieldsExtended.wikidataEndpointHint")} size="long">
      <TextInput
        label={t("sourceFormFieldsExtended.endpointUrl")}
        value={WIKIDATA_ENDPOINT}
        readOnly
        data-testid="wikidata-endpoint"
      />
    </FormField>
  );
}
