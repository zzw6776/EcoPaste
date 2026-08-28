import { Button, Drawer } from "antd";
import type { FC } from "react";
import { useTranslation } from "react-i18next";
import AssetImage from "@/components/AssetImage";
import type { ClipboardItem, FileEntry } from "@/types/clipboard";

interface AndroidFileDetailsDrawerProps {
  busyKey: string | null;
  entries: FileEntry[];
  item: ClipboardItem | null;
  onClose: () => void;
  onOpen: (item: ClipboardItem, index: number) => void;
  onSave: (item: ClipboardItem, index: number) => void;
}

/** Android multi-file details with explicit per-file open and export actions. */
const AndroidFileDetailsDrawer: FC<AndroidFileDetailsDrawerProps> = (props) => {
  const { busyKey, entries, item, onClose, onOpen, onSave } = props;
  const { t } = useTranslation("clipboard");

  function handleOpen(index: number) {
    if (!item) return;

    onOpen(item, index);
  }

  function handleSave(index: number) {
    if (!item) return;

    onSave(item, index);
  }

  return (
    <Drawer
      height="70%"
      onClose={onClose}
      open={item !== null}
      placement="bottom"
      title={t("fileAccess.title")}
    >
      <div className="flex flex-col gap-2">
        {entries.map((entry, index) => {
          const disabled = !entry.exists || entry.isDir;
          const openKey = item ? `open:${item.id}:${index}` : "";
          const saveKey = item ? `save:${item.id}:${index}` : "";

          return (
            <div
              className="flex items-center gap-2 rounded-2 border border-ant-border-secondary bg-ant-fill-quaternary p-2"
              key={entry.path}
            >
              <AssetImage className="size-8 shrink-0" src={entry.iconPath} />
              <div className="min-w-0 flex-1">
                <div className="truncate font-medium text-sm">{entry.name}</div>
                <div className="truncate text-ant-secondary text-xs">
                  {entry.isDir
                    ? t("fileAccess.directoryHint")
                    : entry.exists
                      ? entry.path
                      : t("fileAccess.missing")}
                </div>
              </div>
              <Button
                aria-label={t("fileAccess.open")}
                disabled={disabled || busyKey !== null}
                icon={<i className="i-lucide:external-link size-4" />}
                loading={busyKey === openKey}
                onClick={() => {
                  handleOpen(index);
                }}
                size="small"
                title={t("fileAccess.open")}
                type="text"
              />
              <Button
                aria-label={t("fileAccess.saveAs")}
                disabled={disabled || busyKey !== null}
                icon={<i className="i-lucide:download size-4" />}
                loading={busyKey === saveKey}
                onClick={() => {
                  handleSave(index);
                }}
                size="small"
                title={t("fileAccess.saveAs")}
                type="text"
              />
            </div>
          );
        })}
      </div>
    </Drawer>
  );
};

export default AndroidFileDetailsDrawer;
