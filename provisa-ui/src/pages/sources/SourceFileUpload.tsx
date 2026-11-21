// Copyright (c) 2026 Kenneth Stott
// Canary: 052f2155-0076-4753-a6bb-750db9938b04
//
// This source code is licensed under the Business Source License 1.1

// REQ-1997: the Upload button beside a file-based source's path. The source's id names where
// the file is kept, so it is asked for first; the uploaded file's path is then the source's.

import { useState } from "react";
import { Alert, Button, FileButton, Progress, Text } from "@mantine/core";
import { Upload } from "lucide-react";
import { useTranslation } from "react-i18next";
import { uploadSourceFile } from "./sourceUpload";

interface Props {
  sourceId: string;
  type: string;
  uploaded: boolean;
  onUploaded: (path: string) => void;
  /** The file types the picker offers (a file input's `accept`). */
  accept?: string;
}

export function SourceFileUpload({ sourceId, type, uploaded, onUploaded, accept }: Props) {
  const { t } = useTranslation();
  const [share, setShare] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  const send = async (file: File | null) => {
    if (!file) return;
    setError(null);
    setShare(0);
    try {
      const answer = await uploadSourceFile(sourceId, type, file, setShare);
      onUploaded(answer.path);
    } catch (failed) {
      setError((failed as Error).message);
    } finally {
      setShare(null);
    }
  };

  const noId = !sourceId.trim();
  return (
    <div className="source-file-upload">
      <FileButton onChange={send} accept={accept} disabled={noId || share !== null}>
        {(props) => (
          <Button
            {...props}
            variant="light"
            leftSection={<Upload size={14} />}
            loading={share !== null}
            disabled={noId}
            data-testid="source-file-upload"
          >
            {t(uploaded ? "sourceFormFields.uploadReplace" : "sourceFormFields.upload")}
          </Button>
        )}
      </FileButton>
      {noId && (
        <Text size="xs" c="dimmed" data-testid="source-file-upload-needs-id">
          {t("sourceFormFields.uploadNeedsId")}
        </Text>
      )}
      {share !== null && <Progress value={share * 100} size="sm" mt={4} />}
      {error && (
        <Alert color="red" mt={4} data-testid="source-file-upload-error">
          {error}
        </Alert>
      )}
    </div>
  );
}
