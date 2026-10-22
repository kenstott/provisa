// Copyright (c) 2026 Kenneth Stott
// Canary: 52e8429a-c958-4b38-abb6-52e8f3732b59
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Every 3D view's export, one control on its stage: an image of the view as it is (PNG, JPEG,
// WebP where the browser encodes it, SVG where the scene draws flat), at 1x or 2x, or a video of
// one full turn about it (MP4 or WebM, whichever the browser records), with its progress and a
// cancel. Files are named after the view and the day (exportImage.ts).

import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Button, Menu, Progress, SegmentedControl, Text } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Download } from "lucide-react";
import { Tip } from "../../../components/Tip";
import {
  exportFileName,
  exportImage,
  rasterFormats,
  recordOrbit,
  saveFile,
  videoFormats,
  type ExportableScene,
  type ImageFormat,
  type VideoFormat,
} from "./exportImage";

const LENGTHS = ["4", "8", "15"];
const FPS = 30;

export function ExportMenu({
  scene,
  stage,
  name,
  testId,
}: {
  scene: ExportableScene | null;
  stage: React.RefObject<HTMLDivElement | null>;
  /** The view's name, the files' too (the graph and its view: "ontology-taxonomy"). */
  name: string;
  testId: string;
}) {
  const { t } = useTranslation();
  const [scale, setScale] = useState("1");
  const [seconds, setSeconds] = useState("8");
  const [recording, setRecording] = useState<number | null>(null);
  const abort = useRef<AbortController | null>(null);
  // What the browser encodes, asked once the menu is first opened.
  const [formats, setFormats] = useState<{
    raster: Exclude<ImageFormat, "svg">[];
    video: VideoFormat[];
  } | null>(null);

  const fail = (e: unknown) =>
    notifications.show({
      color: "red",
      message: t("profile3d.exportFailed", { error: e instanceof Error ? e.message : String(e) }),
    });

  const image = (format: ImageFormat) => {
    if (!scene || !stage.current) return;
    exportImage(scene, stage.current, format, Number(scale)).then(
      (blob) => saveFile(blob, exportFileName(name, format === "jpeg" ? "jpg" : format)),
      fail,
    );
  };

  const record = (format: VideoFormat) => {
    if (!scene || !stage.current) return;
    const controller = new AbortController();
    abort.current = controller;
    setRecording(0);
    recordOrbit(scene, stage.current, {
      seconds: Number(seconds),
      fps: FPS,
      format,
      scale: Number(scale),
      onProgress: setRecording,
      signal: controller.signal,
    })
      .then((blob) => saveFile(blob, exportFileName(name, format.ext)))
      .catch((e: unknown) => {
        // A cancel is the reader's choice, not a failure.
        if (!(e instanceof DOMException && e.name === "AbortError")) fail(e);
      })
      .finally(() => {
        abort.current = null;
        setRecording(null);
      });
  };

  if (recording !== null) {
    return (
      <div className="p3d-export p3d-recording" data-testid={`${testId}-recording`}>
        <Text size="xs">{t("profile3d.recording", { percent: Math.round(recording * 100) })}</Text>
        <Progress value={recording * 100} size="sm" w={140} />
        <Button
          size="compact-xs"
          variant="default"
          onClick={() => abort.current?.abort()}
          data-testid={`${testId}-recording-cancel`}
        >
          {t("profile3d.cancelRecording")}
        </Button>
      </div>
    );
  }
  const { raster, video } = formats ?? { raster: [], video: [] };
  return (
    <div className="p3d-export">
      <Menu
        position="bottom-start"
        withinPortal
        closeOnItemClick
        transitionProps={{ duration: 0 }}
        onOpen={() => {
          if (!formats) setFormats({ raster: rasterFormats(), video: videoFormats() });
        }}
      >
        <Menu.Target>
          <Tip text={t("profile3d.export")} named>
            <ActionIcon
              size="sm"
              variant="default"
              disabled={!scene}
              aria-label={t("profile3d.export")}
              data-testid={`${testId}-export`}
            >
              <Download size={14} aria-hidden />
            </ActionIcon>
          </Tip>
        </Menu.Target>
        <Menu.Dropdown>
          <Menu.Label>{t("profile3d.exportImage")}</Menu.Label>
          <div className="p3d-export-choice">
            <SegmentedControl
              size="xs"
              value={scale}
              onChange={setScale}
              data={[
                { value: "1", label: "1×" },
                { value: "2", label: "2×" },
              ]}
              aria-label={t("profile3d.exportScale")}
              data-testid={`${testId}-export-scale`}
            />
          </div>
          {raster.map((f) => (
            <Menu.Item key={f} onClick={() => image(f)} data-testid={`${testId}-export-${f}`}>
              {t(`profile3d.format_${f}`)}
            </Menu.Item>
          ))}
          {scene?.canVector && (
            <Menu.Item onClick={() => image("svg")} data-testid={`${testId}-export-svg`}>
              {t("profile3d.format_svg")}
            </Menu.Item>
          )}
          {video.length > 0 && (
            <>
              <Menu.Divider />
              <Menu.Label>{t("profile3d.recordRotation")}</Menu.Label>
              <div className="p3d-export-choice">
                <SegmentedControl
                  size="xs"
                  value={seconds}
                  onChange={setSeconds}
                  data={LENGTHS.map((s) => ({
                    value: s,
                    label: t("profile3d.seconds", { count: Number(s) }),
                  }))}
                  aria-label={t("profile3d.recordLength")}
                  data-testid={`${testId}-record-length`}
                />
              </div>
              {video.map((f) => (
                <Menu.Item
                  key={f.ext}
                  onClick={() => record(f)}
                  data-testid={`${testId}-record-${f.ext}`}
                >
                  {t(`profile3d.format_${f.ext}`)}
                </Menu.Item>
              ))}
            </>
          )}
        </Menu.Dropdown>
      </Menu>
    </div>
  );
}
