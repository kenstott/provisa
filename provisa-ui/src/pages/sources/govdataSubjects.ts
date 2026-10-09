// Copyright (c) 2026 Kenneth Stott
// Canary: ce73cdf5-b7b6-463b-8b25-6092b95d82b4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-540, REQ-541: an AskAmerica source's subjects and the schemas each brings. The list is
// the server's (admin `govdataSubjects`); nothing here names a subject or a schema.

export interface GovdataSubject {
  value: string;
  label: string;
  schemas: string[];
}

export interface GovdataSubjectCatalog {
  subjects: GovdataSubject[];
  /** Schemas every AskAmerica source serves, whatever subjects it is given. */
  linkerSchemas: string[];
}

/** The schemas a source given `selected` subjects serves: each subject's, then the linker
    schemas, once each. A subject the server does not offer is refused by name. */
export function schemasForSubjects(catalog: GovdataSubjectCatalog, selected: string[]): string[] {
  const schemas: string[] = [];
  for (const value of selected) {
    const subject = catalog.subjects.find((s) => s.value === value);
    if (!subject) throw new Error(`Unknown AskAmerica subject: ${value}`);
    for (const schema of subject.schemas) if (!schemas.includes(schema)) schemas.push(schema);
  }
  for (const schema of catalog.linkerSchemas) if (!schemas.includes(schema)) schemas.push(schema);
  return schemas;
}

/** The subjects a stored schema list was saved from: every subject one of whose own schemas
    is stored. The linker schemas say nothing, since every source has them. */
export function subjectsOfSchemas(catalog: GovdataSubjectCatalog, stored: string[]): string[] {
  const own = stored.filter((schema) => !catalog.linkerSchemas.includes(schema));
  return catalog.subjects
    .filter((subject) => subject.schemas.some((schema) => own.includes(schema)))
    .map((subject) => subject.value);
}
