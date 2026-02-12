import React, { useEffect, useMemo, useState } from "react";
import * as XLSX from "xlsx-js-style";
import MetersTable from "./components/MetersTable";

type ApartmentItem = {
  id: number;
  title: string;
  address?: string | null;
  electric_expected?: number | null;
  tenant_since?: string | null;
  rent_monthly?: number | null;
  utilities_mode?: "by_actual_monthly" | "fixed_monthly" | "quarterly_advance" | null;
  utilities_fixed_monthly?: number | null;
  utilities_advance_amount?: number | null;
  utilities_advance_cycle_months?: number | null;
  utilities_advance_anchor_ym?: string | null;
  utilities_show_actual_to_tenant?: boolean | null;
  has_active_chat?: boolean;
  statuses?: {
    all_photos_received: boolean;
    meters_photo: boolean;
    rent_paid: boolean;
    meters_paid: boolean;
  };
};

type ApartmentsResp = { ok: boolean; ym: string; items: ApartmentItem[] };

type HistoryResp = {
  apartment_id: number;
  history: Array<{
    month: string;
    utilities?: {
      mode?: string;
      actual_accrual?: number | null;
      planned_due?: number | null;
      carry_balance?: number | null;
    };
    meters: {
      cold: { title: string; current: number | null; previous: number | null; delta: number | null; source?: string | null };
      hot: { title: string; current: number | null; previous: number | null; delta: number | null; source?: string | null };
      electric: {
        title: string;
        t1: { title: string; current: number | null; previous: number | null; delta: number | null; source?: string | null };
        t2: { title: string; current: number | null; previous: number | null; delta: number | null; source?: string | null };
        t3: { title: string; current: number | null; previous: number | null; delta: number | null; source?: string | null };
      };
      sewer: { title: string; current: number | null; previous: number | null; delta: number | null; source?: string | null };
    };
  }>;
};

type TariffItem = {
  ym_from: string;
  cold: number;
  hot: number;
  sewer: number;

  // совместимость со старым форматом
  electric: number;

  // новый формат (T3 тариф НЕ используем, но поле может быть в БД)
  electric_t1?: number;
  electric_t2?: number;
  electric_t3?: number;
  created_at?: string;
  updated_at?: string;
};

type TariffsResp = { ok: boolean; items: TariffItem[] };

type ApartmentTariffItem = {
  ym_from: string; // YYYY-MM
  cold: number | null;
  hot: number | null;
  sewer: number | null;
  electric: number | null; // совместимость
  electric_t1: number | null;
  electric_t2: number | null;
  electric_t3: number | null;
  rent?: number | null;
  created_at?: string;
  updated_at?: string;
};

type ApartmentTariffsResp = { ok: boolean; apartment_id: number; items: ApartmentTariffItem[] };

type BillState = {
  pending?: any;
  last?: any;
  approved_at?: string | null;
  sent_at?: string | null;
};

type BillResp = {
  ok: boolean;
  apartment_id: number;
  ym: string;
  bill: any;
  state: BillState;
};

type ReviewFlagItem = {
  id: number;
  apartment_id: number;
  ym: string;
  meter_type: string;
  meter_index: number;
  status: string;
  reason?: string | null;
  comment?: string | null;
};
type ReviewFlagsResp = { ok: boolean; apartment_id: number; items: ReviewFlagItem[] };


type UnassignedPhoto = {
  id: number;
  chat_id: string | null;
  telegram_username: string | null;
  phone: string | null;
  ydisk_path: string;
  status: string;
  apartment_id: number | null;
  created_at: string;
  ocr_json?: any;
};

type UnassignedResp = { ok: boolean; items: UnassignedPhoto[] };

type NotificationItem = {
  id: number;
  created_at: string | null;
  read_at: string | null;
  status: "unread" | "read" | string;
  chat_id: string | null;
  telegram_username: string;
  apartment_id: number | null;
  apartment_title: string | null;
  type: string;
  message: string;
  related?: any;
};

type NotificationsResp = { ok: boolean; items: NotificationItem[]; unread_count: number };

type ApartmentCardResp = {
  ok: boolean;
  apartment: {
    id: number;
    title: string;
    address?: string | null;
    tenant_name?: string | null;
    note?: string | null;
    electric_expected?: number | null; // <-- добавили (может приходить, а может нет)
    cold_serial?: string | null;
    hot_serial?: string | null;
    tenant_since?: string | null;
    rent_monthly?: number | null;
    utilities_mode?: "by_actual_monthly" | "fixed_monthly" | "quarterly_advance" | null;
    utilities_fixed_monthly?: number | null;
    utilities_advance_amount?: number | null;
    utilities_advance_cycle_months?: number | null;
    utilities_advance_anchor_ym?: string | null;
    utilities_show_actual_to_tenant?: boolean | null;
  };
  contacts: { phone: string | null; telegram: string | null };
  chats: Array<{ chat_id: string; is_active: boolean; updated_at: string; created_at: string }>;
};

async function apiGet<T>(path: string): Promise<T> {
  const r = await fetch(`/api${path}`);
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json();
}

async function apiPost<T>(path: string, body?: any): Promise<T> {
  const r = await fetch(`/api${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json();
}

function fmt(v: any) {
  if (v === null || v === undefined) return "—";
  return String(v);
}

function fmtNum(v: number | null | undefined, digits = 3) {
  if (v === null || v === undefined) return "—";
  if (!Number.isFinite(v)) return "—";
  return v.toLocaleString(undefined, { maximumFractionDigits: digits });
}

function fmtRub(v: number | null | undefined) {
  if (v === null || v === undefined) return "—";
  if (!Number.isFinite(v)) return "—";
  return v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fmtDateTime(v: string | null | undefined) {
  if (!v) return "—";
  const d = new Date(v);
  if (!Number.isFinite(d.getTime())) return String(v);
  return d.toLocaleString();
}

function numOrNull(s: string): number | null {
  const t = (s ?? "").trim();
  if (!t) return null;
  const v = Number(t.replace(",", "."));
  if (!Number.isFinite(v)) return null;
  return v;
}

function numOrZero(s: string): number {
  const t = (s ?? "").trim();
  if (!t) return 0;
  const v = Number(t.replace(",", "."));
  if (!Number.isFinite(v)) return 0;
  return v;
}

function isYm(v: string) {
  return /^\d{4}-\d{2}$/.test((v || "").trim());
}

function normalizeYmAny(v: string): string | null {
  const s0 = (v || "").trim();
  if (!s0) return null;
  const s = s0.replace(/[./_]/g, "-");
  let m = s.match(/^(\d{4})-(\d{1,2})$/);
  if (m) {
    const y = Number(m[1]);
    const mm = Number(m[2]);
    if (y >= 1900 && y <= 2100 && mm >= 1 && mm <= 12) return `${String(y).padStart(4, "0")}-${String(mm).padStart(2, "0")}`;
  }
  m = s.match(/^(\d{1,2})-(\d{4})$/);
  if (m) {
    const mm = Number(m[1]);
    const y = Number(m[2]);
    if (y >= 1900 && y <= 2100 && mm >= 1 && mm <= 12) return `${String(y).padStart(4, "0")}-${String(mm).padStart(2, "0")}`;
  }
  m = s.match(/^(\d{4})(\d{2})$/);
  if (m) {
    const y = Number(m[1]);
    const mm = Number(m[2]);
    if (y >= 1900 && y <= 2100 && mm >= 1 && mm <= 12) return `${String(y).padStart(4, "0")}-${String(mm).padStart(2, "0")}`;
  }
  const nums = s.match(/\d+/g) || [];
  if (nums.length >= 2) {
    let year: number | null = null;
    let month: number | null = null;
    for (const n of nums) {
      if (n.length === 4) {
        const y = Number(n);
        if (y >= 1900 && y <= 2100) {
          year = y;
          break;
        }
      }
    }
    if (year === null && nums[nums.length - 1]?.length === 2) {
      const y2 = Number(nums[nums.length - 1]);
      year = 2000 + y2;
    }
    for (const n of nums) {
      const mm = Number(n);
      if (mm >= 1 && mm <= 12) {
        month = mm;
        break;
      }
    }
    if (year !== null && month !== null) return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}`;
  }
  return null;
}

function addMonths(ym: string, delta: number): string {
  if (!isYm(ym)) return ym;
  const [yStr, mStr] = ym.split("-");
  let y = Number(yStr);
  let m = Number(mStr);
  if (!Number.isFinite(y) || !Number.isFinite(m)) return ym;

  m = m + delta;
  while (m > 12) {
    m -= 12;
    y += 1;
  }
  while (m < 1) {
    m += 12;
    y -= 1;
  }
  const mm = String(m).padStart(2, "0");
  return `${y}-${mm}`;
}

function ymRuLabel(ym: string): string {
  if (!isYm(ym)) return ym;
  const months = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"];
  const y = Number(ym.slice(0, 4));
  const m = Number(ym.slice(5, 7)) - 1;
  if (!Number.isFinite(y) || m < 0 || m > 11) return ym;
  return `${months[m]} ${y}`;
}

function ymDisplay(v: string): string {
  const ym = normalizeYmAny(v || "");
  if (!ym) return v || "—";
  return ymRuLabel(ym);
}

function ymToIndex(ym: string): number | null {
  if (!isYm(ym)) return null;
  return Number(ym.slice(0, 4)) * 12 + Number(ym.slice(5, 7)) - 1;
}

export default function App() {
  const [tab, setTab] = useState<"apartments" | "ops">("apartments");
  const [err, setErr] = useState<string | null>(null);
  const [isMobile, setIsMobile] = useState(false);
  const [uiTheme, setUiTheme] = useState<"light" | "light-cool" | "dark" | "ultra">("light");
  const [indicatorPalette, setIndicatorPalette] = useState<"classic" | "bright">("classic");
  const [indicatorStyle, setIndicatorStyle] = useState<"dot" | "diamond" | "triangles">("dot");
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [notifOpen, setNotifOpen] = useState(false);
  const [notifications, setNotifications] = useState<NotificationItem[]>([]);
  const [notifOffset, setNotifOffset] = useState(0);
  const [notifHasMore, setNotifHasMore] = useState(true);
  const [notifLoading, setNotifLoading] = useState(false);
  const [unreadCount, setUnreadCount] = useState(0);
  const [notifHighlight, setNotifHighlight] = useState<{ ym: string; meter_type: string; meter_index: number } | null>(null);
  const [ocrRunLoading, setOcrRunLoading] = useState(false);
  const [ocrRunMsg, setOcrRunMsg] = useState("");

  const [apartments, setApartments] = useState<ApartmentItem[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const STORAGE_SELECTED_ID = "rent.selectedApartmentId";

  // серверный "текущий месяц"
  const [serverYm, setServerYm] = useState<string>("");

  const selected = useMemo(() => apartments.find((a) => a.id === selectedId) ?? null, [apartments, selectedId]);

  // Apartment info modal
  const [infoOpen, setInfoOpen] = useState(false);
  const [infoLoading, setInfoLoading] = useState(false);
  const [infoTitle, setInfoTitle] = useState("");
  const [infoAddress, setInfoAddress] = useState("");
  const [infoTenantName, setInfoTenantName] = useState("");
  const [infoPhone, setInfoPhone] = useState("");
  const [infoTelegram, setInfoTelegram] = useState("");
  const [infoNote, setInfoNote] = useState("");
  const [infoChats, setInfoChats] = useState<Array<{ chat_id: string; is_active: boolean; updated_at: string; created_at: string }>>([]);
  const [bindChatInput, setBindChatInput] = useState("");
  const [infoColdSerial, setInfoColdSerial] = useState("");
  const [infoHotSerial, setInfoHotSerial] = useState("");
  const [infoTenantSince, setInfoTenantSince] = useState("");
  const [infoRentMonthly, setInfoRentMonthly] = useState("");
  const [infoUtilitiesMode, setInfoUtilitiesMode] = useState<"by_actual_monthly" | "fixed_monthly" | "quarterly_advance">("by_actual_monthly");
  const [infoUtilitiesFixedMonthly, setInfoUtilitiesFixedMonthly] = useState("");
  const [infoUtilitiesAdvanceAmount, setInfoUtilitiesAdvanceAmount] = useState("");
  const [infoUtilitiesAdvanceCycleMonths, setInfoUtilitiesAdvanceCycleMonths] = useState("3");
  const [infoUtilitiesAdvanceAnchorYm, setInfoUtilitiesAdvanceAnchorYm] = useState("");
  const [infoUtilitiesShowActualToTenant, setInfoUtilitiesShowActualToTenant] = useState(false);
  const [infoPos, setInfoPos] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
  const [infoDrag, setInfoDrag] = useState<{ active: boolean; dx: number; dy: number }>({ active: false, dx: 0, dy: 0 });
  const [historyOpen, setHistoryOpen] = useState(false);
  const [showGraph, setShowGraph] = useState(false);
  const [graphFrom, setGraphFrom] = useState<string>("");
  const [graphTo, setGraphTo] = useState<string>("");
  const [graphSeries, setGraphSeries] = useState({
    cold: true,
    hot: true,
    t1: true,
    t2: true,
  });
  const [graphMode, setGraphMode] = useState<"rub" | "reading" | "tariff">("rub");
  const [graphHover, setGraphHover] = useState<{ index: number; x: number; y: number } | null>(null);
  const [photoOpen, setPhotoOpen] = useState(false);
  const [photoUrl, setPhotoUrl] = useState<string>("");
  const [photoTitle, setPhotoTitle] = useState<string>("");
  const [photoLoading, setPhotoLoading] = useState(false);
  const [photoPos, setPhotoPos] = useState<{ x: number; y: number }>({ x: 80, y: 80 });
  const [photoDrag, setPhotoDrag] = useState<{ active: boolean; dx: number; dy: number }>({ active: false, dx: 0, dy: 0 });

  // <-- добавили: сколько фото электро ждём (1..3)
  const [infoElectricExpected, setInfoElectricExpected] = useState<string>("1");

  useEffect(() => {
    const check = () => {
      setIsMobile(window.innerWidth <= 820);
    };
    check();
    window.addEventListener("resize", check);
    return () => window.removeEventListener("resize", check);
  }, []);

  useEffect(() => {
    if (selectedId != null) {
      try {
        localStorage.setItem(STORAGE_SELECTED_ID, String(selectedId));
      } catch {
        // ignore
      }
    }
  }, [selectedId]);

  useEffect(() => {
    try {
      const t = localStorage.getItem("v7rent-theme");
      const p = localStorage.getItem("v7rent-indicator-palette");
      const s = localStorage.getItem("v7rent-indicator-style");
      if (t === "dark" || t === "light" || t === "light-cool" || t === "ultra") setUiTheme(t);
      if (p === "classic" || p === "bright") setIndicatorPalette(p);
      if (s === "dot" || s === "diamond" || s === "triangles") setIndicatorStyle(s);
    } catch {
      // ignore
    }
  }, []);

  useEffect(() => {
    document.body.setAttribute("data-theme", uiTheme);
    document.body.setAttribute("data-indicator-palette", indicatorPalette);
    document.body.setAttribute("data-indicator-style", indicatorStyle);
    try {
      localStorage.setItem("v7rent-theme", uiTheme);
      localStorage.setItem("v7rent-indicator-palette", indicatorPalette);
      localStorage.setItem("v7rent-indicator-style", indicatorStyle);
    } catch {
      // ignore
    }
  }, [uiTheme, indicatorPalette, indicatorStyle]);

  async function openInfo(apartmentId: number) {
    setInfoOpen(true);
    setInfoLoading(true);
    try {
      setErr(null);

      const aLocal = apartments.find((a) => a.id === apartmentId) ?? null;

      const data = await apiGet<ApartmentCardResp>(`/admin/ui/apartments/${apartmentId}/card`);
      setInfoTitle(data.apartment?.title ?? "");
      setInfoAddress((data.apartment?.address ?? "") as any);
      setInfoTenantName((data.apartment?.tenant_name ?? "") as any);
      setInfoNote((data.apartment?.note ?? "") as any);
      setInfoPhone(data.contacts?.phone ?? "");
      setInfoTelegram(data.contacts?.telegram ?? "");
      setInfoColdSerial((data.apartment as any)?.cold_serial ?? "");
      setInfoHotSerial((data.apartment as any)?.hot_serial ?? "");
      setInfoTenantSince((data.apartment as any)?.tenant_since ?? "");
      setInfoRentMonthly(
        (data.apartment as any)?.rent_monthly == null ? "" : String((data.apartment as any)?.rent_monthly)
      );
      setInfoUtilitiesMode((((data.apartment as any)?.utilities_mode ?? "by_actual_monthly") as any));
      setInfoUtilitiesFixedMonthly((data.apartment as any)?.utilities_fixed_monthly == null ? "" : String((data.apartment as any)?.utilities_fixed_monthly));
      setInfoUtilitiesAdvanceAmount((data.apartment as any)?.utilities_advance_amount == null ? "" : String((data.apartment as any)?.utilities_advance_amount));
      setInfoUtilitiesAdvanceCycleMonths((data.apartment as any)?.utilities_advance_cycle_months == null ? "3" : String((data.apartment as any)?.utilities_advance_cycle_months));
      setInfoUtilitiesAdvanceAnchorYm((data.apartment as any)?.utilities_advance_anchor_ym ?? "");
      setInfoUtilitiesShowActualToTenant(Boolean((data.apartment as any)?.utilities_show_actual_to_tenant));
      setInfoChats(data.chats ?? []);
      setBindChatInput("");
      setInfoPos({ x: 0, y: 0 });
      setInfoDrag({ active: false, dx: 0, dy: 0 });

      const ee =
        (data.apartment as any)?.electric_expected ??
        (aLocal as any)?.electric_expected ??
        1;
      setInfoElectricExpected(String(ee ?? 1));
    } catch (e: any) {
      setErr(String(e?.message ?? e));
      setInfoOpen(false);
    } finally {
      setInfoLoading(false);
    }
  }

  async function saveInfo(apartmentId: number) {
    const title = infoTitle.trim();
    if (!title) {
      setErr("Название квартиры обязательно.");
      return;
    }

    // <-- нормализуем 1..3
    const eeRaw = Number((infoElectricExpected ?? "").trim());
    const ee = Math.max(1, Math.min(3, Number.isFinite(eeRaw) ? eeRaw : 1));

    try {
      setErr(null);
      await fetch(`/api/admin/ui/apartments/${apartmentId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title,
          address: infoAddress.trim() || null,
          tenant_name: infoTenantName.trim() || null,
          note: infoNote.trim() || null,
          phone: infoPhone.trim() || null,
          telegram: infoTelegram.trim() || null,
          cold_serial: infoColdSerial.trim() || null,
          hot_serial: infoHotSerial.trim() || null,
          tenant_since: infoTenantSince.trim() || null,
          rent_monthly: numOrNull(infoRentMonthly) ?? 0,
          utilities_mode: infoUtilitiesMode,
          utilities_fixed_monthly: numOrNull(infoUtilitiesFixedMonthly),
          utilities_advance_amount: numOrNull(infoUtilitiesAdvanceAmount),
          utilities_advance_cycle_months: Math.max(2, Math.min(24, Number(infoUtilitiesAdvanceCycleMonths || "3") || 3)),
          utilities_advance_anchor_ym: (infoUtilitiesAdvanceAnchorYm || "").trim() || null,
          utilities_show_actual_to_tenant: Boolean(infoUtilitiesShowActualToTenant),
          electric_expected: ee, // <-- добавили
        }),
      }).then(async (r) => {
        if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
        return r.json();
      });

      await loadApartments(false);
      await loadHistory(apartmentId);

      setInfoOpen(false);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  async function loadNotifications(reset: boolean = false) {
    if (notifLoading) return;
    const limit = 30;
    const offset = reset ? 0 : notifOffset;
    try {
      setNotifLoading(true);
      const data = await apiGet<NotificationsResp>(`/admin/notifications?status=all&limit=${limit}&offset=${offset}`);
      setUnreadCount(Number(data.unread_count || 0));
      if (reset) {
        setNotifications(data.items || []);
      } else {
        setNotifications((prev) => [...prev, ...(data.items || [])]);
      }
      setNotifOffset(offset + (data.items?.length || 0));
      setNotifHasMore((data.items?.length || 0) >= limit);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    } finally {
      setNotifLoading(false);
    }
  }

  async function refreshUnreadCount() {
    try {
      const data = await apiGet<NotificationsResp>(`/admin/notifications?status=unread&limit=1&offset=0`);
      setUnreadCount(Number(data.unread_count || 0));
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  async function markNotificationRead(id: number) {
    try {
      await apiPost(`/admin/notifications/${id}/read`, {});
      setNotifications((prev) => prev.map((n) => (n.id === id ? { ...n, status: "read", read_at: n.read_at || new Date().toISOString() } : n)));
      refreshUnreadCount();
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  async function clearReadNotifications() {
    try {
      await apiPost(`/admin/notifications/clear-read`, {});
      await loadNotifications(true);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  async function runOcrDatasetNow() {
    if (ocrRunLoading) return;
    try {
      setOcrRunLoading(true);
      setOcrRunMsg("");
      const resp = await apiPost<{ ok: boolean; message?: string }>(`/admin/ocr-dataset/run`, {});
      setOcrRunMsg(resp?.message || "Запуск инициирован.");
      setTimeout(async () => {
        try {
          const last = await apiGet<{ ok: boolean; message?: string; created_at?: string }>(`/admin/ocr-dataset/last`);
          if (last?.message) {
            setOcrRunMsg(`${last.message}${last.created_at ? ` (${last.created_at})` : ""}`);
          } else {
            setOcrRunMsg("Результат пока не готов.");
          }
        } catch (e: any) {
          setOcrRunMsg("Не удалось получить результат.");
        }
      }, 2000);
    } catch (e: any) {
      setOcrRunMsg("Ошибка запуска OCR-датасета.");
      setErr(String(e?.message ?? e));
    } finally {
      setOcrRunLoading(false);
    }
  }

  async function bindChatToApartment(apartmentId: number) {
    const chatId = bindChatInput.trim();
    if (!chatId) {
      setErr("Введи Telegram ID (chat_id).");
      return;
    }
    try {
      setErr(null);
      await apiPost(`/admin/chats/${encodeURIComponent(chatId)}/bind?apartment_id=${apartmentId}`);
      const data = await apiGet<ApartmentCardResp>(`/admin/ui/apartments/${apartmentId}/card`);
      setInfoChats(data.chats ?? []);
      setInfoRentMonthly((data.apartment as any)?.rent_monthly == null ? "" : String((data.apartment as any)?.rent_monthly));
      await loadApartments(false);
      await loadHistory(apartmentId);
      setBindChatInput("");
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  async function unbindChat(chatId: string, apartmentId: number) {
    try {
      setErr(null);
      await apiPost(`/admin/chats/${encodeURIComponent(chatId)}/unbind`);
      const data = await apiGet<ApartmentCardResp>(`/admin/ui/apartments/${apartmentId}/card`);
      setInfoChats(data.chats ?? []);
      setInfoRentMonthly((data.apartment as any)?.rent_monthly == null ? "" : String((data.apartment as any)?.rent_monthly));
      await loadApartments(false);
      await loadHistory(apartmentId);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  // Add apartment modal
  const [addOpen, setAddOpen] = useState(false);
  const [newTitle, setNewTitle] = useState("");
  const [newAddress, setNewAddress] = useState("");

  // History
  const [history, setHistory] = useState<HistoryResp["history"]>([]);
  const [reviewFlags, setReviewFlags] = useState<ReviewFlagItem[]>([]);
  const [loadingHistory, setLoadingHistory] = useState(false);

  // Edit readings modal
  const [editOpen, setEditOpen] = useState(false);
  const [editMonth, setEditMonth] = useState("");
  const [editCold, setEditCold] = useState("");
  const [editHot, setEditHot] = useState("");
  const [editE1, setEditE1] = useState("");
  const [editE2, setEditE2] = useState("");
  const [editE3, setEditE3] = useState("");

  // Bill state for edit month
  const [billInfo, setBillInfo] = useState<BillResp | null>(null);
  const [billLoading, setBillLoading] = useState(false);
  const [billErr, setBillErr] = useState<string | null>(null);

  // Tariffs
  const [tariffs, setTariffs] = useState<TariffItem[]>([]);
  const [tariffYmFrom, setTariffYmFrom] = useState("");
  const [tariffCold, setTariffCold] = useState("0");
  const [tariffHot, setTariffHot] = useState("0");
  const [tariffElectricT1, setTariffElectricT1] = useState("0");
  const [tariffElectricT2, setTariffElectricT2] = useState("0");
  const [tariffSewer, setTariffSewer] = useState("0");
  const [loadingTariffs, setLoadingTariffs] = useState(false);

  const [globalTariffsOpen, setGlobalTariffsOpen] = useState(false);

  // Apartment-specific tariffs (overrides)
  const [apTariffsOpen, setApTariffsOpen] = useState(false);
  const [apTariffs, setApTariffs] = useState<ApartmentTariffItem[]>([]);
  const [loadingApTariffs, setLoadingApTariffs] = useState(false);

  const [apTariffYmFrom, setApTariffYmFrom] = useState("");
  const [apTariffCold, setApTariffCold] = useState("");
  const [apTariffHot, setApTariffHot] = useState("");
  const [apTariffSewer, setApTariffSewer] = useState("");
  const [apTariffElectricT1, setApTariffElectricT1] = useState("");
  const [apTariffElectricT2, setApTariffElectricT2] = useState("");
  const [apTariffsPos, setApTariffsPos] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
  const [apTariffsDrag, setApTariffsDrag] = useState<{ active: boolean; dx: number; dy: number }>({ active: false, dx: 0, dy: 0 });

  function effectiveTariffForMonth(month: string): {
    cold: number;
    hot: number;
    e1: number;
    e2: number;
    sewer: number;
    ym_from: string | null;
  } {
    const m = (month || "").trim();
    if (!isYm(m) || !tariffs.length) {
      return { cold: 0, hot: 0, e1: 0, e2: 0, sewer: 0, ym_from: null };
    }

    let best: TariffItem | null = null;
    for (const t of tariffs) {
      if (!isYm(t.ym_from)) continue;
      if (t.ym_from <= m) {
        if (!best || best.ym_from < t.ym_from) best = t;
      }
    }

    if (!best) {
      best = tariffs.slice().sort((a, b) => (a.ym_from < b.ym_from ? -1 : a.ym_from > b.ym_from ? 1 : 0))[0] ?? null;
    }

    const baseE = best?.electric ?? 0;
    const e1 = (best?.electric_t1 ?? baseE) as number;
    const e2 = (best?.electric_t2 ?? baseE) as number;

    return {
      cold: best?.cold ?? 0,
      hot: best?.hot ?? 0,
      e1,
      e2,
      sewer: best?.sewer ?? 0,
      ym_from: best?.ym_from ?? null,
    };
  }


  function ymFromAny(t: { ym_from?: string | null; month_from?: string | null }): string {
    return String((t as any).ym_from ?? (t as any).month_from ?? "").trim();
  }

  function effectiveApartmentOverrideForMonth(month: string): {
    cold: number | null;
    hot: number | null;
    e1: number | null;
    e2: number | null;
    e3: number | null;
    sewer: number | null;
    rent: number | null;
    ym_from: string | null;
  } {
    const m = (month || "").trim();
    if (!isYm(m) || !apTariffs.length) {
      return { cold: null, hot: null, e1: null, e2: null, e3: null, sewer: null, rent: null, ym_from: null };
    }

    let best: ApartmentTariffItem | null = null;
    for (const t of apTariffs) {
      const ym = ymFromAny(t as any);
      if (!isYm(ym)) continue;
      if (ym <= m) {
        if (!best || ymFromAny(best as any) < ym) best = t;
      }
    }

    if (!best) {
      best = apTariffs.slice().sort((a, b) => (ymFromAny(a as any) < ymFromAny(b as any) ? -1 : ymFromAny(a as any) > ymFromAny(b as any) ? 1 : 0))[0] ?? null;
    }

    const baseE = best?.electric ?? null;
    const e1 = (best?.electric_t1 ?? baseE) as any;
    const e2 = (best?.electric_t2 ?? baseE) as any;
    const e3 = (best?.electric_t3 ?? null) as any;

    return {
      cold: best?.cold ?? null,
      hot: best?.hot ?? null,
      e1: e1 ?? null,
      e2: e2 ?? null,
      e3,
      sewer: best?.sewer ?? null,
      rent: best?.rent ?? null,
      ym_from: ymFromAny(best as any) || null,
    };
  }

  function effectiveTariffForMonthForSelected(month: string): {
    cold: number;
    hot: number;
    e1: number;
    e2: number;
    sewer: number;
    rent: number | null;
    ym_from: string | null;
    source: "apartment" | "global" | "none";
  } {
    const base = effectiveTariffForMonth(month);
    const ov = effectiveApartmentOverrideForMonth(month);

    const merged = {
      cold: ov.cold != null ? ov.cold : base.cold,
      hot: ov.hot != null ? ov.hot : base.hot,
      e1: ov.e1 != null ? ov.e1 : base.e1,
      e2: ov.e2 != null ? ov.e2 : base.e2,
      sewer: ov.sewer != null ? ov.sewer : base.sewer,
      rent: ov.rent != null ? ov.rent : null,
      ym_from: (ov.ym_from ?? base.ym_from) as any,
      source: (ov.ym_from ? "apartment" : base.ym_from ? "global" : "none") as any,
    };

    return merged;
  }

  // Unassigned photos
  const [unassigned, setUnassigned] = useState<UnassignedPhoto[]>([]);
  const [loadingUnassigned, setLoadingUnassigned] = useState(false);
  const [assignApartmentId, setAssignApartmentId] = useState<number | "">("");
  const [bindChatId, setBindChatId] = useState(true);

  async function loadApartments(selectIfEmpty = true) {
    setErr(null);
    const data = await apiGet<ApartmentsResp>("/admin/ui/apartments");
    setServerYm(data.ym || "");

    const items = (data.items ?? []).map((x) => ({
      id: x.id,
      title: x.title,
      address: x.address ?? null,
      electric_expected: x.electric_expected ?? null,
      tenant_since: (x as any).tenant_since ?? null,
      rent_monthly: (x as any).rent_monthly ?? 0,
      utilities_mode: (x as any).utilities_mode ?? "by_actual_monthly",
      utilities_fixed_monthly: (x as any).utilities_fixed_monthly ?? null,
      utilities_advance_amount: (x as any).utilities_advance_amount ?? null,
      utilities_advance_cycle_months: (x as any).utilities_advance_cycle_months ?? 3,
      utilities_advance_anchor_ym: (x as any).utilities_advance_anchor_ym ?? null,
      utilities_show_actual_to_tenant: Boolean((x as any).utilities_show_actual_to_tenant),
      has_active_chat: Boolean((x as any).has_active_chat),
      statuses: {
        all_photos_received: Boolean((x as any)?.statuses?.all_photos_received),
        meters_photo: Boolean((x as any)?.statuses?.meters_photo),
        rent_paid: Boolean((x as any)?.statuses?.rent_paid),
        meters_paid: Boolean((x as any)?.statuses?.meters_paid),
      },
    }));

    setApartments(items);
    try {
      await apiPost("/admin/ui/rent-reminders/check", {});
    } catch {
      // non-blocking
    }

    if (selectIfEmpty) {
      setSelectedId((prev) => {
        const exists = (id: number | null) => id != null && items.some((i) => i.id === id);
        if (exists(prev)) return prev;
        let stored: number | null = null;
        try {
          const raw = localStorage.getItem(STORAGE_SELECTED_ID);
          const num = raw == null ? NaN : Number(raw);
          stored = Number.isFinite(num) ? num : null;
        } catch {
          stored = null;
        }
        if (exists(stored)) return stored;
        if (items.length) return items[0].id;
        return null;
      });
    }
  }

  async function loadHistory(apartmentId: number) {
    setLoadingHistory(true);
    try {
      setErr(null);
      const [data, flags] = await Promise.all([
        apiGet<HistoryResp>(`/admin/ui/apartments/${apartmentId}/history`),
        apiGet<ReviewFlagsResp>(`/admin/ui/apartments/${apartmentId}/review-flags?status=open`),
      ]);
      setHistory(data.history ?? []);
      setReviewFlags(flags.items ?? []);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
      setHistory([]);
      setReviewFlags([]);
    } finally {
      setLoadingHistory(false);
    }
  }

  async function togglePaidStatus(apartmentId: number, key: "rent_paid" | "meters_paid" | "meters_photo", current: boolean) {
    try {
      setErr(null);
      const ym = (serverYm || "").trim();
      const q = ym ? `?ym=${encodeURIComponent(ym)}` : "";
      const r = await fetch(`/api/admin/ui/apartments/${apartmentId}/statuses${q}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ [key]: !current }),
      });
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      await loadApartments(false);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  async function resolveReviewFlag(flagId: number) {
    try {
      setErr(null);
      await apiPost(`/admin/ui/review-flags/${flagId}/resolve`, {});
      if (selectedId != null) {
        await loadHistory(selectedId);
      }
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  async function createApartment() {
    const title = newTitle.trim();
    if (!title) {
      setErr("Название квартиры обязательно.");
      return;
    }

    try {
      setErr(null);
      const resp = await apiPost<{ ok: boolean; id: number }>("/admin/ui/apartments", {
        title,
        address: newAddress.trim() || null,
      });

      await loadApartments(false);
      setSelectedId(resp.id);

      setAddOpen(false);
      setNewTitle("");
      setNewAddress("");
      setTab("apartments");
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  async function deleteSelectedApartment() {
    if (selectedId == null) {
      setErr("Сначала выбери квартиру слева.");
      return;
    }
    const ok = window.confirm("Удалить квартиру? Это удалит показания и контакты по этой квартире.");
    if (!ok) return;

    try {
      setErr(null);
      const r = await fetch(`/api/admin/ui/apartments/${selectedId}`, { method: "DELETE" });
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);

      await loadApartments(true);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  async function loadTariffs() {
    setLoadingTariffs(true);
    try {
      setErr(null);
      const data = await apiGet<TariffsResp>("/tariffs");
      setTariffs(data.items ?? []);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
      setTariffs([]);
    } finally {
      setLoadingTariffs(false);
    }
  }

  async function saveTariff() {
    const ym = normalizeYmAny(tariffYmFrom);
    if (!ym) {
      setErr("Не удалось распознать месяц. Поддерживаются любые форматы (например: 2026-03, 03/2026, март 2026).");
      return;
    }

    const e1 = numOrZero(tariffElectricT1);
    const e2 = numOrZero(tariffElectricT2);

    const payload = {
      ym_from: ym,
      cold: numOrZero(tariffCold),
      hot: numOrZero(tariffHot),
      sewer: numOrZero(tariffSewer),

      electric: e1, // совместимость
      electric_t1: e1,
      electric_t2: e2,
    };

    try {
      setErr(null);
      await apiPost("/tariffs", payload);
      setTariffYmFrom(ym);
      await loadTariffs();
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }


  async function loadApartmentTariffs(apartmentId: number) {
    try {
      setLoadingApTariffs(true);
      const data = await apiGet<ApartmentTariffsResp>(`/admin/ui/apartments/${apartmentId}/tariffs`);
      setApTariffs((data.items ?? []) as any);
    } catch (e) {
      // если бэкенд ещё без этой фичи — просто не ломаем UI
      setApTariffs([]);
    } finally {
      setLoadingApTariffs(false);
    }
  }

  async function saveApartmentTariff(apartmentId: number) {
    const ym = normalizeYmAny(apTariffYmFrom) ?? normalizeYmAny(serverYm);
    if (!ym) {
      setErr("Не удалось распознать месяц. Поддерживаются любые форматы (например: 2026-03, 03/2026, март 2026).");
      return;
    }

    const payload: any = { month_from: ym };

    const cold = numOrNull(apTariffCold);
    const hot = numOrNull(apTariffHot);
    const sewer = numOrNull(apTariffSewer);
    const e1 = numOrNull(apTariffElectricT1);
    const e2 = numOrNull(apTariffElectricT2);

    if (cold !== null) payload.cold = cold;
    if (hot !== null) payload.hot = hot;
    if (sewer !== null) payload.sewer = sewer;

    // электро: допускаем пустые (не задавать) — тогда наследуем базовые
    if (e1 !== null) payload.electric_t1 = e1;
    if (e2 !== null) payload.electric_t2 = e2;

    try {
      setErr(null);
      await apiPost(`/admin/ui/apartments/${apartmentId}/tariffs`, payload);
      setApTariffYmFrom(ym);
      await loadApartmentTariffs(apartmentId);

      // очистим форму (не трогаем month_from — удобно для серии правок)
      setApTariffCold("");
      setApTariffHot("");
      setApTariffSewer("");
      setApTariffElectricT1("");
      setApTariffElectricT2("");
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  async function loadUnassigned() {
    setLoadingUnassigned(true);
    try {
      setErr(null);
      const data = await apiGet<UnassignedResp>("/admin/photo-events/unassigned");
      setUnassigned(data.items ?? []);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
      setUnassigned([]);
    } finally {
      setLoadingUnassigned(false);
    }
  }

  async function assignPhoto(photoEventId: number) {
    if (!assignApartmentId) {
      setErr("Сначала выбери квартиру в выпадающем списке (куда назначать).");
      return;
    }
    try {
      setErr(null);
      const qs = new URLSearchParams({
        apartment_id: String(assignApartmentId),
        bind_chat_id: bindChatId ? "true" : "false",
      });
      await apiPost(`/admin/photo-events/${photoEventId}/assign?${qs.toString()}`);
      await loadUnassigned();
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  function openEdit(month: string) {
    const row = historyWithFuture.find((h) => h.month === month);
    const m = row?.meters;

    // t3 — информативно
    const t3Fallback =
      m?.electric?.t3?.current ??
      (m?.electric?.t1?.current != null && m?.electric?.t2?.current != null ? (m.electric.t1.current as number) + (m.electric.t2.current as number) : null);

    setEditMonth(month);
    setEditCold(m?.cold?.current == null ? "" : String(m.cold.current));
    setEditHot(m?.hot?.current == null ? "" : String(m.hot.current));
    setEditE1(m?.electric?.t1?.current == null ? "" : String(m.electric.t1.current));
    setEditE2(m?.electric?.t2?.current == null ? "" : String(m.electric.t2.current));
    setEditE3(t3Fallback == null ? "" : String(t3Fallback));


    setEditOpen(true);
    setBillInfo(null);
    setBillErr(null);
    if (selectedId != null) {
      loadBill(selectedId, month).catch(() => {});
    }
  }

  async function loadBill(apartmentId: number, ym: string) {
    setBillLoading(true);
    try {
      const data = await apiGet<BillResp>(`/admin/ui/apartments/${apartmentId}/bill?ym=${encodeURIComponent(ym)}`);
      setBillInfo(data);
      setBillErr(null);
    } catch (e: any) {
      setBillErr(String(e?.message ?? e));
      setBillInfo(null);
    } finally {
      setBillLoading(false);
    }
  }

  async function approveBill(apartmentId: number, ym: string, send: boolean) {
    try {
      setBillErr(null);
      await apiPost(`/admin/ui/apartments/${apartmentId}/bill/approve`, { ym, send });
      await loadBill(apartmentId, ym);
      await loadHistory(apartmentId);
    } catch (e: any) {
      setBillErr(String(e?.message ?? e));
    }
  }

  async function sendBillWithoutT3Photo(apartmentId: number, ym: string) {
    try {
      setBillErr(null);
      await apiPost(`/admin/ui/apartments/${apartmentId}/bill/send-without-t3-photo`, { ym, send: true });
      await loadBill(apartmentId, ym);
      await loadHistory(apartmentId);
    } catch (e: any) {
      setBillErr(String(e?.message ?? e));
    }
  }

  async function saveEdit() {
    if (!selectedId) return;

    const cold = numOrNull(editCold);
    const hot = numOrNull(editHot);
    const e1 = numOrNull(editE1);
    const e2 = numOrNull(editE2);
    const e3 = numOrNull(editE3);

    // пустое поле = "не менять"
    const row = historyWithFuture.find((h) => h.month === editMonth);
    const cur = row?.meters;
    const nearlyEq = (a: number | null | undefined, b: number | null | undefined) => {
      if (a == null && b == null) return true;
      if (a == null || b == null) return false;
      return Math.abs(Number(a) - Number(b)) <= 1e-9;
    };

    try {
      setErr(null);

      const payload: any = { ym: editMonth };
      if (cold !== null && !nearlyEq(cold, cur?.cold?.current ?? null)) payload.cold = cold;
      if (hot !== null && !nearlyEq(hot, cur?.hot?.current ?? null)) payload.hot = hot;
      if (e1 !== null && !nearlyEq(e1, cur?.electric?.t1?.current ?? null)) payload.electric_t1 = e1;
      if (e2 !== null && !nearlyEq(e2, cur?.electric?.t2?.current ?? null)) payload.electric_t2 = e2;
      if (e3 !== null && !nearlyEq(e3, cur?.electric?.t3?.current ?? null)) payload.electric_t3 = e3; // t3 — только если реально изменен

      if (Object.keys(payload).length <= 1) {
        setErr("Нечего сохранять: все поля пустые.");
        return;
      }

      await apiPost(`/admin/ui/apartments/${selectedId}/meters`, payload);
      await loadHistory(selectedId);
      await loadBill(selectedId, editMonth);
      setEditOpen(false);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  useEffect(() => {
    loadApartments(true).catch((e: any) => setErr(String(e?.message ?? e)));
    loadTariffs().catch(() => {});
    loadUnassigned().catch(() => {});
    refreshUnreadCount().catch(() => {});
  }, []);

  useEffect(() => {
    if (selectedId != null) {
      loadHistory(selectedId);
      loadApartmentTariffs(selectedId).catch(() => {});
    }
  }, [selectedId]);

  // Добавляем "следующий месяц" в историю
  const historyWithFuture = useMemo(() => {
    const h = [...(history ?? [])];

    if (!h.length) {
      if (isYm(serverYm)) {
        return [
          {
            month: serverYm,
            meters: {
              cold: { title: "ХВС", current: null, previous: null, delta: null },
              hot: { title: "ГВС", current: null, previous: null, delta: null },
              electric: {
                title: "Электро",
                t1: { title: "T1", current: null, previous: null, delta: null },
                t2: { title: "T2", current: null, previous: null, delta: null },
                t3: { title: "T3", current: null, previous: null, delta: null },
              },
              sewer: { title: "Водоотведение", current: null, previous: null, delta: null },
            },
          } as any,
        ];
      }
      return h;
    }

    const lastMonth = h[h.length - 1]?.month;
    const nextMonth = isYm(lastMonth) ? addMonths(lastMonth, 1) : "";
    if (isYm(nextMonth) && !h.some((x) => x.month === nextMonth)) {
      h.push({
        month: nextMonth,
        meters: {
          cold: { title: "ХВС", current: null, previous: null, delta: null },
          hot: { title: "ГВС", current: null, previous: null, delta: null },
          electric: {
            title: "Электро",
            t1: { title: "T1", current: null, previous: null, delta: null },
            t2: { title: "T2", current: null, previous: null, delta: null },
            t3: { title: "T3", current: null, previous: null, delta: null },
          },
          sewer: { title: "Водоотведение", current: null, previous: null, delta: null },
        },
      } as any);
    }

    return h;
  }, [history, serverYm]);

  const currentYm = isYm(serverYm) ? serverYm : null;
  const latest = currentYm ? historyWithFuture.find((h) => h.month === currentYm) : null;
  const latestMonth = currentYm;
  const latestMeters = latest?.meters ?? null;

  function emptyMonthRow(month: string) {
    return {
      month,
      meters: {
        cold: { title: "ХВС", current: null, previous: null, delta: null },
        hot: { title: "ГВС", current: null, previous: null, delta: null },
        electric: {
          title: "Электро",
          t1: { title: "T1", current: null, previous: null, delta: null },
          t2: { title: "T2", current: null, previous: null, delta: null },
          t3: { title: "T3", current: null, previous: null, delta: null },
        },
        sewer: { title: "Водоотведение", current: null, previous: null, delta: null },
      },
    } as any;
  }

  const last4 = useMemo(() => {
    const realLast = (history?.length ? history[history.length - 1]?.month : null) || (isYm(serverYm) ? serverYm : null);
    if (!realLast || !isYm(realLast)) {
      return historyWithFuture.slice(-4).reverse();
    }

    const months = [addMonths(realLast, -2), addMonths(realLast, -1), realLast, addMonths(realLast, 1)];
    return months.map((m) => historyWithFuture.find((h) => h.month === m) || emptyMonthRow(m)).reverse();
  }, [history, historyWithFuture, serverYm]);

  const allHistoryMonths = useMemo(() => {
    return (history ?? []).map((h) => h.month).filter(isYm).sort();
  }, [history]);

  useEffect(() => {
    if (!allHistoryMonths.length) return;
    setGraphFrom((prev) => (prev && isYm(prev) ? prev : allHistoryMonths[0]));
    setGraphTo((prev) => (prev && isYm(prev) ? prev : allHistoryMonths[allHistoryMonths.length - 1]));
  }, [allHistoryMonths]);

  const graphMonths = useMemo(() => {
    if (!graphFrom || !graphTo) return allHistoryMonths;
    const from = graphFrom <= graphTo ? graphFrom : graphTo;
    const to = graphFrom <= graphTo ? graphTo : graphFrom;
    return allHistoryMonths.filter((m) => m >= from && m <= to);
  }, [allHistoryMonths, graphFrom, graphTo]);

  function getSeriesValue(h: HistoryResp["history"][number], kind: "cold" | "hot" | "t1" | "t2") {
    const t = effectiveTariffForMonthForSelected(h.month);
    if (graphMode === "tariff") {
      if (kind === "cold") return t.cold ?? null;
      if (kind === "hot") return t.hot ?? null;
      if (kind === "t1") return t.e1 ?? null;
      return t.e2 ?? null;
    }
    if (graphMode === "reading") {
      if (kind === "cold") return h?.meters?.cold?.current ?? null;
      if (kind === "hot") return h?.meters?.hot?.current ?? null;
      if (kind === "t1") return h?.meters?.electric?.t1?.current ?? null;
      return h?.meters?.electric?.t2?.current ?? null;
    }
    // rub
    if (kind === "cold") {
      const d = h?.meters?.cold?.delta ?? null;
      return d == null ? null : d * (t.cold || 0);
    }
    if (kind === "hot") {
      const d = h?.meters?.hot?.delta ?? null;
      return d == null ? null : d * (t.hot || 0);
    }
    if (kind === "t1") {
      const d = h?.meters?.electric?.t1?.delta ?? null;
      return d == null ? null : d * (t.e1 || 0);
    }
    const d = h?.meters?.electric?.t2?.delta ?? null;
    return d == null ? null : d * (t.e2 || 0);
  }

  const graphData = useMemo(() => {
    const byMonth = new Map((history ?? []).map((h) => [h.month, h]));
    const items = graphMonths.map((m) => {
      const h = byMonth.get(m);
      return {
        month: m,
        cold: h ? getSeriesValue(h, "cold") : null,
        hot: h ? getSeriesValue(h, "hot") : null,
        t1: h ? getSeriesValue(h, "t1") : null,
        t2: h ? getSeriesValue(h, "t2") : null,
      };
    });
    return items;
  }, [graphMonths, history, graphMode]);

  const utilitiesByMonth = useMemo(() => {
    const m = new Map<string, { actual: number | null; planned: number | null; carry: number | null }>();
    for (const h of history || []) {
      const ym = String((h as any)?.month || "");
      const u = (h as any)?.utilities || {};
      m.set(ym, {
        actual: u?.actual_accrual == null ? null : Number(u.actual_accrual),
        planned: u?.planned_due == null ? null : Number(u.planned_due),
        carry: u?.carry_balance == null ? null : Number(u.carry_balance),
      });
    }
    return m;
  }, [history]);

  async function openMeterPhoto(month: string, meterType: string, meterIndex: number) {
    if (!selectedId) return;
    try {
      setErr(null);
      setPhotoLoading(true);
      setPhotoTitle(`${month} · ${meterType.toUpperCase()}${meterType === "electric" ? ` ${meterIndex}` : ""}`);
      const res = await fetch(
        `/api/admin/ui/apartments/${selectedId}/photo?ym=${encodeURIComponent(month)}&meter_type=${encodeURIComponent(meterType)}&meter_index=${encodeURIComponent(String(meterIndex))}`
      );
      if (!res.ok) throw new Error(`Фото не найдено`);
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      setPhotoUrl((prev) => {
        if (prev) URL.revokeObjectURL(prev);
        return url;
      });
      setPhotoOpen(true);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    } finally {
      setPhotoLoading(false);
    }
  }

  function setGraphRangeByCount(count: number) {
    if (!allHistoryMonths.length) return;
    if (count <= 0) {
      setGraphFrom(allHistoryMonths[0]);
      setGraphTo(allHistoryMonths[allHistoryMonths.length - 1]);
      return;
    }
    const to = allHistoryMonths[allHistoryMonths.length - 1];
    const from = allHistoryMonths[Math.max(0, allHistoryMonths.length - count)];
    setGraphFrom(from);
    setGraphTo(to);
  }

  function renderGraph() {
    const width = 1000;
    const height = 280;
    const padL = 48;
    const padR = 18;
    const padT = 16;
    const padB = 34;

    const seriesKeys: Array<keyof typeof graphSeries> = ["cold", "hot", "t1", "t2"];
    const activeKeys = seriesKeys.filter((k) => graphSeries[k]);
    const values: number[] = [];
    for (const row of graphData) {
      for (const k of activeKeys) {
        const v = row[k];
        if (v != null && Number.isFinite(v)) values.push(Number(v));
      }
    }
    const maxValRaw = values.length ? Math.max(...values) : 1;
    const maxVal = Math.max(1, maxValRaw * 1.15);

    const xStep = graphData.length > 1 ? (width - padL - padR) / (graphData.length - 1) : 1;
    const yScale = (height - padT - padB) / maxVal;

    function y(v: number) {
      return height - padB - v * yScale;
    }

    function linePoints(key: keyof typeof graphSeries) {
      return graphData
        .map((row, i) => {
          const v = row[key];
          if (v == null || !Number.isFinite(v)) return null;
          return `${padL + i * xStep},${y(Number(v))}`;
        })
        .filter(Boolean)
        .join(" ");
    }

    const colors: Record<string, string> = {
      cold: "#2563eb",
      hot: "#ef4444",
      t1: "#111827",
      t2: "#16a34a",
    };

    return (
      <div style={{ position: "relative" }}>
        <svg
          viewBox={`0 0 ${width} ${height}`}
          style={{ width: "100%", height: 280, display: "block" }}
          onMouseMove={(e) => {
            const rect = (e.currentTarget as any).getBoundingClientRect();
            const x = e.clientX - rect.left;
            const i = Math.round((x - padL) / xStep);
            if (i < 0 || i >= graphData.length) {
              setGraphHover(null);
              return;
            }
            const cx = padL + i * xStep;
            setGraphHover({ index: i, x: cx, y: padT });
          }}
          onMouseLeave={() => setGraphHover(null)}
        >
          {[0, 0.33, 0.66, 1].map((t) => {
            const val = maxVal * t;
            const yy = y(val);
            return (
              <g key={t}>
                <line x1={padL} y1={yy} x2={width - padR} y2={yy} stroke="#f1f5f9" />
                <text x={8} y={yy + 4} fontSize={10} fill="#9ca3af">
                  {Math.round(val).toLocaleString()}
                </text>
              </g>
            );
          })}
          <line x1={padL} y1={height - padB} x2={width - padR} y2={height - padB} stroke="#e5e7eb" />
          <line x1={padL} y1={padT} x2={padL} y2={height - padB} stroke="#e5e7eb" />

          {activeKeys.map((k) => {
            const pts = linePoints(k);
            if (!pts) return null;
            return <polyline key={k} fill="none" stroke={colors[k]} strokeWidth={2.5} points={pts} />;
          })}

          {activeKeys.map((k) =>
            graphData.map((row, i) => {
              const v = row[k];
              if (v == null || !Number.isFinite(v)) return null;
              return (
                <circle
                  key={`${k}-${row.month}`}
                  cx={padL + i * xStep}
                  cy={y(Number(v))}
                  r={3}
                  fill={colors[k]}
                  stroke="#fff"
                  strokeWidth={1}
                />
              );
            })
          )}

          {graphHover ? (
            <line
              x1={graphHover.x}
              y1={padT}
              x2={graphHover.x}
              y2={height - padB}
              stroke="#e5e7eb"
              strokeDasharray="4 4"
            />
          ) : null}
        </svg>

        {graphHover ? (
          <div
            style={{
              position: "absolute",
              left: Math.min(Math.max(graphHover.x + 12, 8), 740),
              top: 8,
              background: "white",
              border: "1px solid #e5e7eb",
              borderRadius: 10,
              padding: "8px 10px",
              boxShadow: "0 10px 24px rgba(0,0,0,0.12)",
              fontSize: 12,
              minWidth: 180,
            }}
          >
            <div style={{ fontWeight: 800, marginBottom: 6 }}>{graphData[graphHover.index]?.month}</div>
            {activeKeys.map((k) => (
              <div key={k} style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                <span style={{ color: colors[k] }}>{k.toUpperCase()}</span>
                <span>
                  {graphData[graphHover.index]?.[k] == null ? "—" : fmtRub(Number(graphData[graphHover.index]?.[k]))}
                </span>
              </div>
            ))}
          </div>
        ) : null}

        <div style={{ display: "flex", gap: 16, marginTop: 8, flexWrap: "wrap", fontSize: 12, color: "#444" }}>
          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            {activeKeys.map((k) => (
              <div key={k} style={{ display: "flex", gap: 6, alignItems: "center" }}>
                <span style={{ width: 10, height: 10, borderRadius: 999, background: colors[k], display: "inline-block" }} />
                <span>{k.toUpperCase()}</span>
              </div>
            ))}
          </div>
          {graphData.map((row) => row.month).slice(0, 6).map((m) => (
            <div key={m}>{m}</div>
          ))}
          {graphData.length > 6 ? <div>…</div> : null}
        </div>
      </div>
    );
  }

  function exportHistoryXlsx() {
    const rows = (history ?? []).slice().sort((a, b) => String(a.month).localeCompare(String(b.month)));
    const data: Array<Record<string, any>> = [];
    let carryCalc = 0;
    for (const h of rows) {
      const cold = h?.meters?.cold?.current ?? null;
      const hot = h?.meters?.hot?.current ?? null;
      const t1 = h?.meters?.electric?.t1?.current ?? null;
      const t2 = h?.meters?.electric?.t2?.current ?? null;
      const t3 = h?.meters?.electric?.t3?.current ?? null;
      const sewer = h?.meters?.sewer?.current ?? null;

      const dc = h?.meters?.cold?.delta ?? null;
      const dh = h?.meters?.hot?.delta ?? null;
      const de1 = h?.meters?.electric?.t1?.delta ?? null;
      const de2 = h?.meters?.electric?.t2?.delta ?? null;
      const de3 = h?.meters?.electric?.t3?.delta ?? null;
      const ds = calcSewerDelta(h as any);
      const tariff = effectiveTariffForMonthForSelected(h.month);
      const rc = dc == null ? null : dc * (tariff.cold || 0);
      const rh = dh == null ? null : dh * (tariff.hot || 0);
      const re1 = de1 == null ? null : de1 * (tariff.e1 || 0);
      const re2 = de2 == null ? null : de2 * (tariff.e2 || 0);
      const rs = ds == null ? null : ds * (tariff.sewer || 0);
      const sum = calcSumRub(rc, rh, re1, re2, rs);
      const rent = effectiveRentForMonth(h.month);
      const actualAccrual = (h as any)?.utilities?.actual_accrual ?? sum;
      const plannedDue = (h as any)?.utilities?.planned_due ?? plannedUtilitiesForMonth(h.month, actualAccrual);
      const carryBalance = (h as any)?.utilities?.carry_balance;
      if (carryBalance == null) {
        carryCalc = carryCalc + Number(plannedDue || 0) - Number(actualAccrual || 0);
      } else {
        carryCalc = Number(carryBalance || 0);
      }

      data.push({
        "Месяц": h.month,
        "ХВС (показание)": cold,
        "ХВС Δ": dc,
        "ХВС ₽": rc,
        "ГВС (показание)": hot,
        "ГВС Δ": dh,
        "ГВС ₽": rh,
        "T1 (показание)": t1,
        "T1 Δ": de1,
        "T1 ₽": re1,
        "T2 (показание)": t2,
        "T2 Δ": de2,
        "T2 ₽": re2,
        "T3 (показание)": t3,
        "T3 Δ": de3,
        "Водоотведение (показание)": sewer,
        "Водоотведение Δ": ds,
        "Водоотведение ₽": rs,
        "Начислено факт ₽": actualAccrual,
        "К оплате ₽": plannedDue,
        "Баланс переноса ₽": carryCalc,
        "Аренда ₽": rent > 0 ? rent : null,
        "Сумма ₽": sum,
      });
    }

    const ws = XLSX.utils.json_to_sheet(data, {
      header: [
        "Месяц",
        "ХВС (показание)",
        "ХВС Δ",
        "ХВС ₽",
        "ГВС (показание)",
        "ГВС Δ",
        "ГВС ₽",
        "T1 (показание)",
        "T1 Δ",
        "T1 ₽",
        "T2 (показание)",
        "T2 Δ",
        "T2 ₽",
        "T3 (показание)",
        "T3 Δ",
        "Водоотведение (показание)",
        "Водоотведение Δ",
        "Водоотведение ₽",
        "Начислено факт ₽",
        "К оплате ₽",
        "Баланс переноса ₽",
        "Аренда ₽",
        "Сумма ₽",
      ],
    });
    const headerRow = 1;
    const headerStyle = {
      font: { bold: true, color: { rgb: "111111" } },
      fill: { fgColor: { rgb: "EEF2F7" } },
      alignment: { vertical: "center", horizontal: "center", wrapText: true },
      border: {
        top: { style: "thin", color: { rgb: "D1D5DB" } },
        bottom: { style: "thin", color: { rgb: "D1D5DB" } },
        left: { style: "thin", color: { rgb: "D1D5DB" } },
        right: { style: "thin", color: { rgb: "D1D5DB" } },
      },
    } as any;
    const bodyStyle = {
      font: { color: { rgb: "111111" } },
      alignment: { vertical: "center", horizontal: "center" },
      fill: { fgColor: { rgb: "F8FAFC" } },
      border: {
        top: { style: "thin", color: { rgb: "E5E7EB" } },
        bottom: { style: "thin", color: { rgb: "E5E7EB" } },
        left: { style: "thin", color: { rgb: "E5E7EB" } },
        right: { style: "thin", color: { rgb: "E5E7EB" } },
      },
    } as any;

    const range = XLSX.utils.decode_range(ws["!ref"] || "A1:A1");
    for (let C = range.s.c; C <= range.e.c; C++) {
      const cell = XLSX.utils.encode_cell({ r: headerRow - 1, c: C });
      if (ws[cell]) ws[cell].s = headerStyle;
    }

    for (let R = range.s.r + 1; R <= range.e.r; R++) {
      for (let C = range.s.c; C <= range.e.c; C++) {
        const cell = XLSX.utils.encode_cell({ r: R, c: C });
        if (ws[cell]) ws[cell].s = bodyStyle;
      }
    }

    ws["!freeze"] = { xSplit: 0, ySplit: 1 };
    ws["!cols"] = [
      { wch: 10 },
      { wch: 14 },
      { wch: 10 },
      { wch: 10 },
      { wch: 14 },
      { wch: 10 },
      { wch: 10 },
      { wch: 14 },
      { wch: 10 },
      { wch: 10 },
      { wch: 14 },
      { wch: 10 },
      { wch: 10 },
      { wch: 14 },
      { wch: 10 },
      { wch: 18 },
      { wch: 12 },
      { wch: 14 },
      { wch: 16 },
      { wch: 14 },
      { wch: 18 },
      { wch: 12 },
      { wch: 12 },
    ];
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "История");
    XLSX.writeFile(wb, `history_${selected?.title ?? "apartment"}.xlsx`);
  }
  // сколько столбцов электро показывать (T1/T2/T3)
  const eN = Math.max(1, Math.min(3, Number((selected as any)?.electric_expected ?? 1) || 1));
  const tableColsNoAction = eN + 5; // month + cold + hot + sewer + Tn + sum
  const summaryGridTemplate = `repeat(${tableColsNoAction}, minmax(0, 1fr)) 56px`;
  const summarySumCol = eN + 5; // same as table sum col index
  const summaryRentCol = Math.max(2, Math.floor((summarySumCol + 1) / 3));
  const summaryCountersCol = Math.max(summaryRentCol + 1, Math.min(summarySumCol - 1, Math.floor((2 * summarySumCol) / 3)));

  function getReviewFlag(month: string, meterType: string, meterIndex: number): ReviewFlagItem | null {
    const mt = String(meterType || "").toLowerCase();
    const mi = Number(meterIndex || 1);
    return (
      reviewFlags.find(
        (f) =>
          String(f.ym || "") === String(month || "") &&
          String(f.meter_type || "").toLowerCase() === mt &&
          Number(f.meter_index || 1) === mi &&
          String(f.status || "").toLowerCase() === "open"
      ) || null
    );
  }

  function calcSewerDelta(h: HistoryResp["history"][number]) {
    const d = h?.meters?.sewer?.delta;
    if (d != null && Number.isFinite(d)) return d;

    const dc = h?.meters?.cold?.delta ?? 0;
    const dh = h?.meters?.hot?.delta ?? 0;
    const sum = (Number.isFinite(dc) ? dc : 0) + (Number.isFinite(dh) ? dh : 0);
    return sum || null;
  }

  function calcElectricT3Fallback(h: HistoryResp["history"][number]): { current: number | null; delta: number | null } {
    const e3c = h?.meters?.electric?.t3?.current ?? null;
    const e3d = h?.meters?.electric?.t3?.delta ?? null;
    return { current: e3c, delta: e3d };
  }


  function cellTriplet(
    current: number | null,
    delta: number | null,
    rub: number | null,
    tariff: number | null,
    rubEnabled: boolean,
    highlightMode: "none" | "missing" | "review" = "none"
  ) {
    const color = highlightMode === "review" ? "#b91c1c" : highlightMode === "missing" ? "#d97706" : "var(--text)";
    return (
      <div className="meter-triplet" style={{ display: "grid", gap: 2, lineHeight: 1.25 }}>
        <div className="meter-main" style={{ color, fontSize: 13, fontWeight: 400 }}>{fmtNum(current, 3)}</div>
        <div className="meter-rub" style={{ color: "var(--muted)", fontSize: 12 }}>{rubEnabled ? (rub == null ? "₽ —" : `₽ ${fmtRub(rub)}`) : "₽ —"}</div>
        <div className="meter-delta" style={{ color: "var(--muted)", fontSize: 12 }}>Δ {fmtNum(delta, 3)}</div>
        <div className="meter-tariff" style={{ color: "var(--muted)", fontSize: 11 }}>тариф: {tariff == null ? "—" : fmtNum(tariff, 3)}</div>
      </div>
    );
  }

  function calcSumRub(rc: number | null, rh: number | null, re1: number | null, re2: number | null, rs: number | null) {
    const parts = [rc, rh, re1, re2, rs].filter((x) => x != null && Number.isFinite(x)) as number[];
    if (!parts.length) return null;
    return parts.reduce((a, b) => a + b, 0);
  }

  function effectiveRentForMonth(ym: string): number {
    if (!selected) return 0;
    const rent = Number((selected as any)?.rent_monthly ?? 0);
    if (!Number.isFinite(rent) || rent <= 0) return 0;
    if (!Boolean((selected as any)?.has_active_chat)) return 0;
    const fromYm = normalizeYmAny(String((selected as any)?.tenant_since ?? ""));
    if (fromYm && isYm(ym) && ym < fromYm) return 0;
    return rent;
  }

  function plannedUtilitiesForMonth(ym: string, actual: number | null): number | null {
    if (!selected) return actual;
    if (!isYm(ym)) return actual;
    const mode = String((selected as any)?.utilities_mode ?? "by_actual_monthly");
    const tenantSinceYm = normalizeYmAny(String((selected as any)?.tenant_since ?? ""));
    if (tenantSinceYm && ym < tenantSinceYm) return 0;

    if (mode === "fixed_monthly") {
      const fixed = Number((selected as any)?.utilities_fixed_monthly ?? 0);
      return Number.isFinite(fixed) && fixed > 0 ? fixed : 0;
    }
    if (mode === "quarterly_advance") {
      const amount = Number((selected as any)?.utilities_advance_amount ?? 0);
      const cycle = Math.max(2, Number((selected as any)?.utilities_advance_cycle_months ?? 3) || 3);
      const anchor = normalizeYmAny(String((selected as any)?.utilities_advance_anchor_ym ?? "")) || tenantSinceYm || ym;
      const yi = ymToIndex(ym);
      const ai = ymToIndex(anchor);
      const isStart = yi != null && ai != null ? ((yi - ai) % cycle === 0) : true;
      return isStart && Number.isFinite(amount) && amount > 0 ? amount : 0;
    }
    return actual;
  }

  function rentDueInfoForMonth(ym: string): { day: number | null; text: string; overdue: boolean } {
    if (!selected) return { day: null, text: "—", overdue: false };
    const tenantSince = String((selected as any)?.tenant_since ?? "").trim();
    const m = tenantSince.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return { day: null, text: "—", overdue: false };
    const dueDay = Math.max(1, Math.min(31, Number(m[3])));
    const text = `${dueDay}-ое число`;
    const ymOk = isYm(ym);
    if (!ymOk) return { day: dueDay, text, overdue: false };
    const now = new Date();
    const y = Number(ym.slice(0, 4));
    const mm = Number(ym.slice(5, 7));
    const dueDate = new Date(y, mm - 1, Math.min(dueDay, new Date(y, mm, 0).getDate()));
    const grace = new Date(dueDate);
    grace.setDate(grace.getDate() + 1);
    const isUnpaid = !Boolean((selected as any)?.statuses?.rent_paid);
    const overdue = isUnpaid && now > grace && ym === serverYm;
    return { day: dueDay, text, overdue };
  }

  // Для карточек вверху (последний месяц)
  const latestTariff = useMemo(() => (latestMonth ? effectiveTariffForMonthForSelected(latestMonth) : null), [latestMonth, tariffs, apTariffs]);
  const latestRowComputed = useMemo(() => {
    if (!latest || !latestTariff) return { counters: null, sum: null };
    const h = latest as any;

    const dc = h?.meters?.cold?.delta ?? null;
    const dh = h?.meters?.hot?.delta ?? null;
    const de1 = h?.meters?.electric?.t1?.delta ?? null;
    const de2 = h?.meters?.electric?.t2?.delta ?? null;
    const ds = calcSewerDelta(h);

    const rc = dc == null ? null : dc * (latestTariff.cold || 0);
    const rh = dh == null ? null : dh * (latestTariff.hot || 0);
    const re1 = de1 == null ? null : de1 * (latestTariff.e1 || 0);
    const re2 = de2 == null ? null : de2 * (latestTariff.e2 || 0);
    const rs = ds == null ? null : ds * (latestTariff.sewer || 0);

    const isComplete =
      h?.meters?.cold?.current != null &&
      h?.meters?.hot?.current != null &&
      h?.meters?.electric?.t1?.current != null &&
      (eN < 2 || h?.meters?.electric?.t2?.current != null) &&
      (eN < 3 || h?.meters?.electric?.t3?.current != null);

    const counters = isComplete ? calcSumRub(rc, rh, re1, re2, rs) : null;
    const rentValue = latestMonth ? effectiveRentForMonth(latestMonth) : 0;
    const rent = Number.isFinite(rentValue) ? rentValue : 0;
    const sum = counters == null ? null : counters + rent;
    return { counters, sum };

  }, [latest, latestTariff, latestMonth, selectedId, apartments]);
  const latestRentDue = useMemo(() => rentDueInfoForMonth(latestMonth || ""), [latestMonth, selectedId, apartments, serverYm]);
  const latestRentAmount = useMemo(() => {
    if (!latestMonth) return 0;
    return effectiveRentForMonth(latestMonth);
  }, [latestMonth, selectedId, apartments]);
  const latestUtilitiesPlanned = useMemo(() => {
    if (!latestMonth) return latestRowComputed.counters;
    const fromHistory = history.find((h) => h.month === latestMonth) as any;
    const plannedHist = fromHistory?.utilities?.planned_due;
    if (plannedHist != null && Number.isFinite(Number(plannedHist))) return Number(plannedHist);
    return plannedUtilitiesForMonth(latestMonth, latestRowComputed.counters);
  }, [latestMonth, history, latestRowComputed.counters, selectedId, apartments]);
  const latestTotalPlanned = useMemo(() => {
    const p = latestUtilitiesPlanned;
    if (p == null) return null;
    return Number(p) + Number(latestRentAmount || 0);
  }, [latestUtilitiesPlanned, latestRentAmount]);
  const isRentUnpaid = Boolean(selected) && !Boolean((selected as any)?.statuses?.rent_paid);
  const rentAlertColor = indicatorStyle === "triangles" ? "var(--warn-bright)" : "var(--warn-active)";

  function renderStatusSwitch(
    checked: boolean,
    onToggle?: () => void,
    readOnly: boolean = false
  ) {
    return (
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          if (!readOnly && onToggle) onToggle();
        }}
        className="lamp-btn"
        style={{ cursor: readOnly ? "default" : "pointer", opacity: readOnly ? 0.9 : 1 }}
        aria-label={checked ? "Ок" : "Проблема"}
        title={checked ? "Ок" : "Проблема"}
      >
        <span className={`lamp ${checked ? "ok" : "bad"}`} />
      </button>
    );
  }

  return (
    <>
    <div className="app-shell">
      <div className="topbar-row">
        <h1 className="app-title">V7rent</h1>

        <div className="top-actions">
          <button
            onClick={() => setTab("apartments")}
            className={`action-btn icon-static-btn${tab === "apartments" ? " is-active" : ""}`}
            style={{ borderRadius: 14, width: 38, height: 38, padding: 0 }}
            title="Объекты"
            aria-label="Объекты"
          >
            <svg viewBox="0 0 24 24" width="17" height="17" style={{ display: "block", margin: "0 auto" }}>
              <path d="M4 10.5 12 4l8 6.5" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" fill="none" />
              <path d="M6.5 9.8V20h11V9.8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" fill="none" />
              <path d="M10 20v-5.2h4V20" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" fill="none" />
            </svg>
          </button>

          <button
            onClick={() => setAddOpen(true)}
            className="action-btn"
            style={{ borderRadius: 14, width: 36, height: 36, padding: 0 }}
            title="Добавить квартиру"
            aria-label="Добавить квартиру"
          >
            <svg viewBox="0 0 24 24" width="16" height="16" style={{ display: "block", margin: "0 auto" }}>
              <path d="M12 5v14" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
              <path d="M5 12h14" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
            </svg>
          </button>

          <button
            onClick={() => deleteSelectedApartment()}
            className="action-btn icon-static-btn"
            style={{ borderRadius: 14, width: 36, height: 36, padding: 0 }}
            title="Удалить"
            aria-label="Удалить"
          >
            <svg viewBox="0 0 24 24" width="17" height="17" style={{ display: "block", margin: "0 auto" }}>
              <path d="M9 4.8h6" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
              <path d="M4.8 7.2h14.4" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
              <path d="M7.2 7.2l.8 11.1a1.8 1.8 0 0 0 1.8 1.7h4.4a1.8 1.8 0 0 0 1.8-1.7l.8-11.1" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
              <path d="M10.2 10.4v6.2M13.8 10.4v6.2" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
            </svg>
          </button>

          <button
            onClick={() => setSettingsOpen(true)}
            className={`action-btn icon-static-btn${settingsOpen ? " is-active" : ""}`}
            style={{ width: 38, height: 38, padding: 0 }}
            title="Тема и индикаторы"
          >
            <svg viewBox="0 0 24 24" width="17" height="17" style={{ display: "block", margin: "0 auto", color: "currentColor" }}>
              <path
                d="M12 4.1c-4.4 0-8 3.2-8 7.3 0 4 3.2 7.2 7.2 7.2h1.2c1 0 1.8-.8 1.8-1.8 0-.5-.2-.9-.5-1.3-.5-.5-.7-1-.7-1.4 0-1.1.9-1.9 2-1.9h1.2c2.2 0 3.9-1.7 3.9-3.9C20.1 6 16.8 4.1 12 4.1Z"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.75"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              <circle cx="8.1" cy="10.2" r="1.05" fill="none" stroke="currentColor" strokeWidth="1.55" />
              <circle cx="11.8" cy="8.1" r="1.05" fill="none" stroke="currentColor" strokeWidth="1.55" />
              <circle cx="15.2" cy="9.4" r="1.05" fill="none" stroke="currentColor" strokeWidth="1.55" />
              <circle cx="7.1" cy="13.6" r="1.05" fill="none" stroke="currentColor" strokeWidth="1.55" />
            </svg>
          </button>

          <button
            onClick={() => {
              const next = !notifOpen;
              setNotifOpen(next);
              if (next) loadNotifications(true);
            }}
            className="action-btn icon-static-btn"
            style={{ position: "relative", width: 38, height: 38, padding: 0 }}
            title="Уведомления"
          >
            <svg viewBox="0 0 24 24" width="17" height="17" style={{ display: "block", margin: "0 auto", color: "currentColor" }}>
              <path d="M7.2 16.2h9.6" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" />
              <path d="M8.2 16.2v-4.8a3.8 3.8 0 1 1 7.6 0v4.8" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" />
              <path d="M5.8 16.2h12.4" fill="none" stroke="currentColor" strokeWidth="1.35" strokeLinecap="round" opacity="0.75" />
              <path d="M10.4 18.8a1.7 1.7 0 0 0 3.2 0" fill="none" stroke="currentColor" strokeWidth="1.45" strokeLinecap="round" />
            </svg>
            {unreadCount > 0 ? (
              <span
                style={{
                  position: "absolute",
                  top: 6,
                  right: 6,
                  width: 10,
                  height: 10,
                  borderRadius: "50%",
                  background: "#d946ef",
                  boxShadow: "0 0 0 2px white",
                }}
              />
            ) : null}
          </button>

          <button
            onClick={() => setTab("ops")}
            className={`action-btn${tab === "ops" ? " is-active" : ""}`}
            style={{ borderRadius: 14 }}
          >
            Тарифы
          </button>
        </div>

        {notifOpen ? (
          <>
            <div
              onClick={() => setNotifOpen(false)}
              style={{
                position: "fixed",
                inset: 0,
                background: "transparent",
                zIndex: 40,
              }}
            />
            <div
              style={{
                position: "absolute",
                right: 0,
                top: 50,
                width: 360,
                maxWidth: "90vw",
                background: "white",
                border: "1px solid #eee",
                borderRadius: 12,
                boxShadow: "0 10px 30px rgba(0,0,0,0.12)",
                padding: 10,
                zIndex: 50,
              }}
            >
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
                <div style={{ fontWeight: 900 }}>Уведомления</div>
                <div style={{ display: "flex", gap: 6 }}>
                  <button
                    onClick={() => runOcrDatasetNow()}
                    style={{
                      padding: "6px 8px",
                      borderRadius: 8,
                      border: "1px solid #ddd",
                      background: "white",
                      cursor: "pointer",
                      fontWeight: 800,
                      fontSize: 12,
                      opacity: ocrRunLoading ? 0.6 : 1,
                    }}
                  >
                    {ocrRunLoading ? "Запуск..." : "Собрать OCR"}
                  </button>
                  <button
                    onClick={() => clearReadNotifications()}
                    style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 800, fontSize: 12 }}
                  >
                    Очистить прочитанные
                  </button>
                </div>
              </div>
              {ocrRunMsg ? <div style={{ color: "#666", fontSize: 12, marginBottom: 6 }}>{ocrRunMsg}</div> : null}

            <div
              style={{
                maxHeight: 360,
                overflowY: "auto",
                display: "flex",
                flexDirection: "column",
                gap: 8,
              }}
              onScroll={(e) => {
                const el = e.currentTarget;
                if (!notifHasMore || notifLoading) return;
                if (el.scrollTop + el.clientHeight >= el.scrollHeight - 10) {
                  loadNotifications(false);
                }
              }}
            >
              {!notifications.length ? (
                <div style={{ color: "#666" }}>Нет уведомлений.</div>
              ) : (
                notifications.map((n) => {
                  const unread = String(n.status || "") !== "read";
                  return (
                    <div
                      key={n.id}
                      onClick={() => {
                        if (unread) markNotificationRead(n.id);
                        if (n.apartment_id) {
                          setTab("apartments");
                          setSelectedId(n.apartment_id);
                        }
                        if (n.related?.ym && n.related?.meter_type) {
                          setNotifHighlight({
                            ym: String(n.related.ym),
                            meter_type: String(n.related.meter_type),
                            meter_index: Number(n.related.meter_index || 1),
                          });
                        } else {
                          setNotifHighlight(null);
                        }
                      }}
                      style={{
                        border: "1px solid #eee",
                        borderRadius: 10,
                        padding: 10,
                        background: unread ? "#fff7fb" : "white",
                        cursor: "pointer",
                        display: "grid",
                        gap: 6,
                      }}
                    >
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
                        <div style={{ fontWeight: 900 }}>{n.telegram_username || "Без username"}</div>
                        <div style={{ color: "#666", fontSize: 11 }}>{n.created_at ? new Date(n.created_at).toLocaleString() : ""}</div>
                      </div>
                      <div style={{ color: "#111" }}>{n.message}</div>
                      <div style={{ color: "#666", fontSize: 12 }}>
                        Квартира: {n.apartment_title || (n.apartment_id ? `#${n.apartment_id}` : "—")}
                      </div>
                    </div>
                  );
                })
              )}
              {notifLoading ? <div style={{ color: "#666", fontSize: 12 }}>Загрузка…</div> : null}
            </div>
          </div>
          </>
        ) : null}
      </div>

      {err ? (
        <div style={{ marginTop: 12, background: "#fff2f2", border: "1px solid #ffd0d0", color: "#8a0000", padding: 12, borderRadius: 12 }}>
          <div style={{ fontWeight: 900 }}>Ошибка</div>
          <div style={{ marginTop: 6, whiteSpace: "pre-wrap" }}>{err}</div>
        </div>
      ) : null}

      {tab === "apartments" ? (
        <>
        <div className="section-head" style={{ marginTop: 8 }}>
          <div className="panel-title">Квартиры</div>
          <div className="toolbar">
            <button disabled={!selected} className={`tool-btn tool-icon-btn${infoOpen ? " is-active" : ""}`} onClick={() => selected && openInfo(selected.id)} title="Инфо" aria-pressed={infoOpen}>
              <svg viewBox="0 0 24 24" width="16" height="16"><path d="M12 16v-4" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/><path d="M12 8h.01" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/><path d="M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0z" fill="none" stroke="currentColor" strokeWidth="2"/></svg>
            </button>
            <button
              disabled={!selected}
              className={`tool-btn tool-icon-btn${apTariffsOpen ? " is-active" : ""}`}
              onClick={() => {
                if (!selected) return;
                if (!normalizeYmAny(apTariffYmFrom) && normalizeYmAny(serverYm)) {
                  setApTariffYmFrom(String(serverYm));
                }
                setApTariffsOpen(true);
                loadApartmentTariffs(selected.id).catch(() => {});
              }}
              title="Тарифы"
              aria-pressed={apTariffsOpen}
            >
              <svg viewBox="0 0 24 24" width="16" height="16"><path d="M4 20V4" stroke="currentColor" strokeWidth="2"/><path d="M4 20h16" stroke="currentColor" strokeWidth="2"/><path d="M8 20v-7" stroke="currentColor" strokeWidth="2"/><path d="M12 20v-11" stroke="currentColor" strokeWidth="2"/><path d="M16 20v-5" stroke="currentColor" strokeWidth="2"/><path d="M20 20v-14" stroke="currentColor" strokeWidth="2"/></svg>
            </button>
            <button disabled={!selected} className={`tool-btn tool-icon-btn${historyOpen ? " is-active" : ""}`} onClick={() => selected && setHistoryOpen(true)} title="История" aria-pressed={historyOpen}>
              <svg viewBox="0 0 24 24" width="16" height="16"><path d="M3 12a9 9 0 1 0 3-6.7" fill="none" stroke="currentColor" strokeWidth="2"/><path d="M3 4v4h4" stroke="currentColor" strokeWidth="2"/><path d="M12 7v5l3 2" stroke="currentColor" strokeWidth="2"/></svg>
            </button>
            <button disabled={!selected} className={`tool-btn tool-icon-btn${showGraph ? " is-active" : ""}`} onClick={() => setShowGraph((v) => !v)} title="Графики" aria-pressed={showGraph}>
              <svg viewBox="0 0 24 24" width="16" height="16"><path d="M4 16l5-5 4 4 7-7" fill="none" stroke="currentColor" strokeWidth="2"/><path d="M20 7v5h-5" fill="none" stroke="currentColor" strokeWidth="2"/></svg>
            </button>
          </div>
        </div>
        <div
          style={{
            marginTop: 0,
            display: "grid",
            gridTemplateColumns: isMobile ? "1fr" : "320px 1fr",
            gap: isMobile ? 12 : 14,
            alignItems: "start",
          }}
        >
          {/* LEFT */}
          <div className="panel" style={{ order: 0 }}>

            {!apartments.length ? (
              <div style={{ color: "#666" }}>Пока нет квартир. Нажми “+ Квартира”.</div>
            ) : (
              <div className="apartments-list">
                {apartments.map((a) => {
                  const active = a.id === selectedId;
                  return (
                    <button
                      key={a.id}
                      onClick={() => setSelectedId(a.id)}
                      className={`apt-card${active ? " is-active" : ""}`}
                    >
                      <div className="apt-title">{a.title}</div>
                      {a.address ? <div className="apt-address">{a.address}</div> : null}
                      <div className="apt-rows">
                        <div className="apt-row">
                          <div className="apt-row-label">Фото счетчиков</div>
                          {renderStatusSwitch(
                            Boolean(a.statuses?.meters_photo),
                            () => togglePaidStatus(a.id, "meters_photo", Boolean(a.statuses?.meters_photo)),
                            false
                          )}
                        </div>
                        <div className="apt-row">
                          <div className="apt-row-label">Оплата аренды</div>
                          {renderStatusSwitch(
                            Boolean(a.statuses?.rent_paid),
                            () => togglePaidStatus(a.id, "rent_paid", Boolean(a.statuses?.rent_paid)),
                            false
                          )}
                        </div>
                        <div className="apt-row">
                          <div className="apt-row-label">Оплата счетчиков</div>
                          {renderStatusSwitch(
                            Boolean(a.statuses?.meters_paid),
                            () => togglePaidStatus(a.id, "meters_paid", Boolean(a.statuses?.meters_paid)),
                            false
                          )}
                        </div>
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </div>

          {/* RIGHT */}
          <div className="panel" style={{ order: isMobile ? 1 : 0 }}>
            <div className="panel-title" style={{ visibility: "hidden", height: 0, margin: 0 }} />

            {!selected ? (
              <div style={{ color: "#666" }}>Выбери квартиру слева или создай новую.</div>
            ) : loadingHistory ? (
              <div style={{ color: "#666" }}>Загрузка...</div>
            ) : !historyWithFuture.length ? (
              <div style={{ color: "#666" }}>Пока нет показаний по этой квартире.</div>
            ) : (
              <>
                <div
                  className="summary-strip"
                  style={{
                    gridTemplateColumns: summaryGridTemplate,
                  }}
                >
                  <div className="summary-cell summary-cell-month" style={{ gridColumn: "1 / span 1" }}>
                    <div className="summary-label ghost-label">&nbsp;</div>
                    <div className="summary-value">{ymRuLabel(latestMonth)}</div>
                  </div>
                  <div className="summary-cell summary-cell-rent" style={{ gridColumn: `${summaryRentCol} / span 1`, textAlign: "center" }}>
                    <div className="summary-label">Аренда</div>
                    <div className="summary-label" style={{ marginTop: 2 }}>{latestRentDue.text}</div>
                    <div
                      className="summary-value"
                      style={{ marginTop: 4, color: isRentUnpaid ? rentAlertColor : undefined }}
                      title={isRentUnpaid ? "Аренда не оплачена" : undefined}
                    >
                      {latestRentAmount > 0 ? fmtRub(latestRentAmount) : "—"}
                    </div>
                  </div>
                  <div className="summary-cell summary-cell-counters" style={{ gridColumn: `${summaryCountersCol} / span 1`, textAlign: "center" }}>
                    <div className="summary-label">Счетчики</div>
                    <div className="summary-value">
                      {latestUtilitiesPlanned == null ? "—" : `${fmtRub(latestUtilitiesPlanned)}`}
                    </div>
                  </div>
                  <div
                    className="summary-cell summary-cell-total"
                    style={{ gridColumn: `${summarySumCol} / span 1`, textAlign: "center" }}
                  >
                    <div className="summary-label">Сумма</div>
                    <div className="summary-value">
                      {latestTotalPlanned == null ? "—" : `${fmtRub(latestTotalPlanned)}`}
                    </div>
                  </div>
                  <div className="summary-cell summary-cell-actions-spacer" style={{ gridColumn: `${summarySumCol + 1} / span 1` }} />
                </div>

                <div className="right-stack">
                  {showGraph ? (
                    <div>
                      <div style={{ display: "flex", gap: 16, alignItems: "center", flexWrap: "wrap", marginBottom: 12 }}>
                        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                          <button
                            onClick={() => setGraphMode("rub")}
                            style={{ padding: "6px 8px", borderRadius: 8, border: graphMode === "rub" ? "2px solid #111" : "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 700 }}
                          >
                            ₽
                          </button>
                          <button
                            onClick={() => setGraphMode("reading")}
                            style={{ padding: "6px 8px", borderRadius: 8, border: graphMode === "reading" ? "2px solid #111" : "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 700 }}
                          >
                            Показания
                          </button>
                          <button
                            onClick={() => setGraphMode("tariff")}
                            style={{ padding: "6px 8px", borderRadius: 8, border: graphMode === "tariff" ? "2px solid #111" : "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 700 }}
                          >
                            Тарифы
                          </button>
                        </div>

                        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
                          <input type="checkbox" checked={graphSeries.cold} onChange={(e) => setGraphSeries((s) => ({ ...s, cold: e.target.checked }))} />
                          ХВС
                        </label>
                        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
                          <input type="checkbox" checked={graphSeries.hot} onChange={(e) => setGraphSeries((s) => ({ ...s, hot: e.target.checked }))} />
                          ГВС
                        </label>
                        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
                          <input type="checkbox" checked={graphSeries.t1} onChange={(e) => setGraphSeries((s) => ({ ...s, t1: e.target.checked }))} />
                          T1
                        </label>
                        <label style={{ display: "flex", gap: 6, alignItems: "center" }}>
                          <input type="checkbox" checked={graphSeries.t2} onChange={(e) => setGraphSeries((s) => ({ ...s, t2: e.target.checked }))} />
                          T2
                        </label>

                        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                          <button onClick={() => setGraphRangeByCount(3)} style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 700 }}>
                            3м
                          </button>
                          <button onClick={() => setGraphRangeByCount(6)} style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 700 }}>
                            6м
                          </button>
                          <button onClick={() => setGraphRangeByCount(12)} style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 700 }}>
                            12м
                          </button>
                          <button onClick={() => setGraphRangeByCount(0)} style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 700 }}>
                            Все
                          </button>
                        </div>

                        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                          <span style={{ color: "#666" }}>c</span>
                          <select value={graphFrom} onChange={(e) => setGraphFrom(e.target.value)} style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid #ddd" }}>
                            {allHistoryMonths.map((m) => (
                              <option key={m} value={m}>{m}</option>
                            ))}
                          </select>
                          <span style={{ color: "#666" }}>по</span>
                          <select value={graphTo} onChange={(e) => setGraphTo(e.target.value)} style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid #ddd" }}>
                            {allHistoryMonths.map((m) => (
                              <option key={m} value={m}>{m}</option>
                            ))}
                          </select>
                        </div>
                      </div>

                      {renderGraph()}
                    </div>
                  ) : (
                    <MetersTable
                      rows={last4}
                      eN={eN}
                      currentYm={serverYm}
                      showRentColumn={false}
                      showPolicyColumns={false}
                      effectiveTariffForMonth={(m) => effectiveTariffForMonthForSelected(m)}
                      calcSewerDelta={calcSewerDelta}
                      calcElectricT3Fallback={calcElectricT3Fallback}
                      cellTriplet={cellTriplet}
                      calcSumRub={calcSumRub}
                      fmtRub={fmtRub}
                      openEdit={openEdit}
                      getReviewFlag={getReviewFlag}
                      onResolveReviewFlag={resolveReviewFlag}
                      notificationHighlight={notifHighlight}
                      onCellPhoto={openMeterPhoto}
                    />
                  )}
                </div>
              </>
            )}
          </div>
        </div>
        </>
      ) : (
        <div className="ops-root" style={{ marginTop: 16, display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>

          {/* Tariffs (global) */}
          <div className="ops-card" style={{ borderRadius: 12, padding: 12 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12 }}>
              <div>
                <div style={{ fontWeight: 900 }}>Тарифы (по умолчанию)</div>
                <div style={{ marginTop: 6, color: "#666", fontSize: 13 }}>
                  Базовые тарифы применяются ко всем квартирам, если у квартиры нет переопределения.
                </div>
              </div>
                <button
                  className="action-btn is-primary"
                  onClick={async () => {
                    setGlobalTariffsOpen(true);
                    await loadTariffs();
                  }}
                  style={{ padding: "8px 12px", borderRadius: 10, border: "1px solid #111", background: "#111", color: "white", cursor: "pointer", fontWeight: 900 }}
              >
                Открыть
              </button>
            </div>
          </div>

          {/* Unassigned photos */}
          <div className="ops-card" style={{ borderRadius: 12, padding: 12 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div style={{ fontWeight: 900 }}>Неразобранные фото</div>
              <button className="action-btn" onClick={() => loadUnassigned()} style={{ padding: "8px 12px", borderRadius: 10, cursor: "pointer" }}>
                Обновить
              </button>
            </div>

            <div style={{ marginTop: 10, display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
              <div className="ops-label">Куда назначать:</div>
              <div className="ops-select-wrap">
                <select
                  className="ops-select"
                  value={assignApartmentId}
                  onChange={(e) => setAssignApartmentId(e.target.value ? Number(e.target.value) : "")}
                >
                  <option value="">— выбери квартиру —</option>
                  {apartments.map((a) => (
                    <option key={a.id} value={a.id}>
                      {a.title}
                    </option>
                  ))}
                </select>
                <span className="ops-select-arrow" aria-hidden="true">▾</span>
              </div>

              <label className="ops-switch">
                <input type="checkbox" checked={bindChatId} onChange={(e) => setBindChatId(e.target.checked)} />
                <span className="ops-switch-track" aria-hidden="true">
                  <span className="ops-switch-thumb" />
                </span>
                <span className="ops-switch-text">Привязать chat_id (если найден)</span>
              </label>
            </div>

            <div style={{ marginTop: 10, color: "#666", fontSize: 13 }}>
              “Неразобранные” = фото, которые ещё не привязались к квартире автоматически.
            </div>

            <div style={{ marginTop: 10 }}>
              {loadingUnassigned ? (
                <div style={{ color: "#666" }}>Загрузка...</div>
              ) : !unassigned.length ? (
                <div style={{ color: "#666" }}>Неразобранных фото нет</div>
              ) : (
                <table style={{ width: "100%", borderCollapse: "collapse" }}>
                  <thead>
                    <tr>
                      <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>ID</th>
                      <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Когда</th>
                      <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Файл/путь</th>
                      <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>OCR</th>
                      <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Действия</th>
                    </tr>
                  </thead>
                  <tbody>
                    {unassigned.map((p) => (
                      <tr key={p.id}>
                        <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{p.id}</td>
                        <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{p.created_at}</td>
                        <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2", maxWidth: 360, wordBreak: "break-all" }}>{p.ydisk_path}</td>
                        <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                          {p.ocr_json ? (
                            <span>
                              {fmt(p.ocr_json?.type)}: {fmt(p.ocr_json?.reading)} (conf {fmt(p.ocr_json?.confidence)})
                            </span>
                          ) : (
                            "—"
                          )}
                        </td>
                        <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                          <button
                            onClick={() => assignPhoto(p.id)}
                            style={{
                              padding: "8px 10px",
                              borderRadius: 10,
                              border: "1px solid #111",
                              background: "#111",
                              color: "white",
                              cursor: "pointer",
                              fontWeight: 900,
                            }}
                          >
                            Назначить
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Edit readings modal */}
      {editOpen && selected && (
        <div
          onClick={() => setEditOpen(false)}
          className="modal-overlay"
        >
          <div
            onClick={(e) => e.stopPropagation()}
            className="modal-shell"
            style={{ width: 600, maxWidth: "96vw", maxHeight: "92vh", overflow: "auto", fontSize: 13 }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 10 }}>
              <div style={{ fontWeight: 900, fontSize: 18 }}>Редактировать показания: {editMonth}</div>
              <button onClick={() => setEditOpen(false)} style={{ padding: "8px 10px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 900 }}>
                Закрыть
              </button>
            </div>

            <div style={{ marginTop: 12, display: "grid", gap: 10 }}>
              <div style={{ display: "grid", gap: 10, gridTemplateColumns: "1fr 1fr" }}>
                <label style={{ display: "grid", gap: 6 }}>
                  <div style={{ fontWeight: 800 }}>ХВС</div>
                  <input value={editCold} onChange={(e) => setEditCold(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                </label>
                <label style={{ display: "grid", gap: 6 }}>
                  <div style={{ fontWeight: 800 }}>ГВС</div>
                  <input value={editHot} onChange={(e) => setEditHot(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                </label>
              </div>

              <div style={{ display: "grid", gap: 10, gridTemplateColumns: "1fr 1fr 1fr" }}>
                <label style={{ display: "grid", gap: 6 }}>
                  <div style={{ fontWeight: 800 }}>Электро T1</div>
                  <input value={editE1} onChange={(e) => setEditE1(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                </label>
                <label style={{ display: "grid", gap: 6 }}>
                  <div style={{ fontWeight: 800 }}>Электро T2</div>
                  <input value={editE2} onChange={(e) => setEditE2(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                </label>
                <label style={{ display: "grid", gap: 6 }}>
                  <div style={{ fontWeight: 800 }}>Электро T3 (итого, без тарифа)</div>
                  <input value={editE3} onChange={(e) => setEditE3(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                </label>
              </div>

              <div style={{ color: "#666", fontSize: 12 }}>Если поле оставить пустым — оно не изменится. Можно вводить с точкой или запятой.</div>

              <div style={{ borderTop: "1px solid #eee", paddingTop: 12, marginTop: 4 }}>
                <div style={{ fontWeight: 900, marginBottom: 6 }}>Согласование суммы</div>

                {billLoading ? (
                  <div style={{ color: "#666" }}>Загрузка расчёта...</div>
                ) : billErr ? (
                  <div style={{ color: "#8a0000" }}>{billErr}</div>
                ) : billInfo?.bill ? (
                  <div style={{ display: "grid", gap: 6 }}>
                    <div>Причина: <b>{String(billInfo.bill?.reason ?? "—")}</b></div>
                    <div>Сумма: <b>{billInfo.bill?.total_rub == null ? "—" : `₽ ${fmtRub(billInfo.bill?.total_rub)}`}</b></div>
                    <div>Approved: {billInfo.state?.approved_at ? "да" : "нет"}; Sent: {billInfo.state?.sent_at ? "да" : "нет"}</div>

                    {Array.isArray(billInfo.bill?.missing) && billInfo.bill.missing.length ? (
                      <div>Не хватает: {billInfo.bill.missing.join(", ")}</div>
                    ) : null}

                    {billInfo.bill?.pending_items && Object.keys(billInfo.bill.pending_items).length ? (
                      <div>
                        Есть превышения по статьям:{" "}
                        {Object.keys(billInfo.bill.pending_items).join(", ")}
                      </div>
                    ) : null}

                    {Array.isArray(billInfo.bill?.pending_flags) && billInfo.bill.pending_flags.length ? (
                      <div>
                        Флаги: {billInfo.bill.pending_flags.map((f: any) => f.code || "flag").join(", ")}
                      </div>
                    ) : null}

                    {billInfo.bill?.reason === "pending_admin" ? (
                      <div style={{ display: "flex", gap: 8, marginTop: 4 }}>
                        <button
                          onClick={() => approveBill(selected.id, editMonth, true)}
                          style={{
                            padding: "8px 10px",
                            borderRadius: 10,
                            border: "1px solid #111",
                            background: "#111",
                            color: "white",
                            cursor: "pointer",
                            fontWeight: 900,
                          }}
                        >
                          Согласовать и отправить
                        </button>
                        <button
                          onClick={() => approveBill(selected.id, editMonth, false)}
                          style={{
                            padding: "8px 10px",
                            borderRadius: 10,
                            border: "1px solid #ddd",
                            background: "white",
                            cursor: "pointer",
                            fontWeight: 900,
                          }}
                        >
                          Согласовать без отправки
                        </button>
                      </div>
                    ) : null}

                    {billInfo.bill?.reason === "missing_photos" &&
                    Array.isArray(billInfo.bill?.missing) &&
                    billInfo.bill.missing.length === 1 &&
                    billInfo.bill.missing[0] === "electric_3" ? (
                      <div style={{ display: "flex", gap: 8, marginTop: 4 }}>
                        <button
                          onClick={() => sendBillWithoutT3Photo(selected.id, editMonth)}
                          style={{
                            padding: "8px 10px",
                            borderRadius: 10,
                            border: "1px solid #111",
                            background: "#111",
                            color: "white",
                            cursor: "pointer",
                            fontWeight: 900,
                          }}
                        >
                          Отправить сумму без фото T3
                        </button>
                      </div>
                    ) : null}
                  </div>
                ) : (
                  <div style={{ color: "#666" }}>Нет данных по сумме.</div>
                )}
              </div>

              <div style={{ display: "flex", gap: 10, justifyContent: "flex-end" }}>
                <button onClick={() => setEditOpen(false)} style={{ padding: "10px 12px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 900 }}>
                  Отмена
                </button>
                <button onClick={saveEdit} style={{ padding: "10px 12px", borderRadius: 10, border: "1px solid #111", background: "#111", color: "white", cursor: "pointer", fontWeight: 900 }}>
                  Сохранить
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {historyOpen && selected && (
        <div
          onClick={() => setHistoryOpen(false)}
          className="modal-overlay"
          style={{ zIndex: 60 }}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            className="modal-shell"
            style={{ width: 1100, maxWidth: "96vw", maxHeight: "92vh", overflow: "auto" }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, marginBottom: 12 }}>
              <div style={{ fontWeight: 900, fontSize: 18 }}>История показаний</div>
              <div style={{ display: "flex", gap: 8 }}>
                <button
                  onClick={() => exportHistoryXlsx()}
                  style={{ padding: "8px 10px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 800 }}
                >
                  Выгрузить Excel
                </button>
                <button
                  onClick={() => setHistoryOpen(false)}
                  style={{ padding: "8px 10px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 800 }}
                >
                  Закрыть
                </button>
              </div>
            </div>

            <MetersTable
              rows={historyWithFuture.slice().reverse()}
              eN={eN}
              showRentColumn={true}
              rentForMonth={(m) => effectiveRentForMonth(m)}
              showPolicyColumns={true}
              utilitiesForMonth={(m) => utilitiesByMonth.get(m) || null}
              effectiveTariffForMonth={(m) => effectiveTariffForMonthForSelected(m)}
              calcSewerDelta={calcSewerDelta}
              calcElectricT3Fallback={calcElectricT3Fallback}
              cellTriplet={cellTriplet}
              calcSumRub={calcSumRub}
              fmtRub={fmtRub}
              openEdit={openEdit}
              getReviewFlag={getReviewFlag}
              onResolveReviewFlag={resolveReviewFlag}
              notificationHighlight={notifHighlight}
              onCellPhoto={openMeterPhoto}
            />
          </div>
        </div>
      )}

      {photoOpen && (
        <div
          onMouseMove={(e) => {
            if (!photoDrag.active) return;
            setPhotoPos({ x: e.clientX - photoDrag.dx, y: e.clientY - photoDrag.dy });
          }}
          onMouseUp={() => setPhotoDrag({ active: false, dx: 0, dy: 0 })}
          className="modal-overlay modal-overlay-transparent"
          style={{ zIndex: 80, pointerEvents: "auto" }}
        >
          <div
            className="modal-shell modal-photo-shell"
            style={{ position: "absolute", left: photoPos.x, top: photoPos.y, width: 520, maxWidth: "90vw" }}
          >
            <div
              onMouseDown={(e) => {
                setPhotoDrag({ active: true, dx: e.clientX - photoPos.x, dy: e.clientY - photoPos.y });
              }}
              style={{
                padding: "8px 10px",
                borderBottom: "1px solid #e5e7eb",
                cursor: "move",
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                fontWeight: 800,
              }}
            >
              <div>{photoTitle || "Фото"}</div>
              <button
                onClick={() => {
                  if (photoUrl) URL.revokeObjectURL(photoUrl);
                  setPhotoUrl("");
                  setPhotoOpen(false);
                }}
                style={{ border: "1px solid #e5e7eb", background: "white", borderRadius: 8, padding: "4px 8px", cursor: "pointer" }}
              >
                Закрыть
              </button>
            </div>
            <div style={{ padding: 10, textAlign: "center" }}>
              {photoLoading ? (
                <div style={{ color: "#666" }}>Загрузка...</div>
              ) : photoUrl ? (
                <img src={photoUrl} alt="meter" style={{ maxWidth: "100%", borderRadius: 8 }} />
              ) : (
                <div style={{ color: "#666" }}>Фото не найдено</div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Apartment info modal */}
      {infoOpen && selected && (
        <div
          onClick={() => setInfoOpen(false)}
          className="modal-overlay"
          onMouseMove={(e) => {
            if (!infoDrag.active) return;
            setInfoPos({ x: e.clientX - infoDrag.dx, y: e.clientY - infoDrag.dy });
          }}
          onMouseUp={() => setInfoDrag({ active: false, dx: 0, dy: 0 })}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            className="modal-shell info-modal"
            onMouseDown={(e) => {
              const t = e.target as HTMLElement;
              if (t.closest("input,select,textarea,button,label,a,[role='button']")) return;
              setInfoDrag({ active: true, dx: e.clientX - infoPos.x, dy: e.clientY - infoPos.y });
            }}
            onKeyDownCapture={(e) => {
              if (e.key !== "Enter" || e.shiftKey) return;
              const t = e.target as HTMLElement;
              if (t.closest("textarea,button")) return;
              e.preventDefault();
              saveInfo(selected.id);
            }}
            style={{
              width: 600,
              maxWidth: "96vw",
              maxHeight: "92vh",
              overflow: "auto",
              fontSize: 13,
              transform: `translate(${infoPos.x}px, ${infoPos.y}px)`,
              cursor: infoDrag.active ? "grabbing" : "grab",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 10 }}>
              <div className="info-modal-title" style={{ fontWeight: 900, fontSize: 18 }}>Карточка квартиры</div>
              <button className="action-btn" onClick={() => setInfoOpen(false)} style={{ padding: "8px 10px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 900 }}>
                Закрыть
              </button>
            </div>

            {infoLoading ? (
              <div style={{ marginTop: 12, color: "#666" }}>Загрузка...</div>
            ) : (
              <div style={{ marginTop: 12, display: "grid", gap: 10 }}>
                <div style={{ display: "grid", gap: 8, gridTemplateColumns: "1fr 1fr" }}>
                  <label style={{ display: "grid", gap: 4 }}>
                    <div style={{ fontWeight: 800 }}>Название квартиры *</div>
                    <input value={infoTitle} onChange={(e) => setInfoTitle(e.target.value)} style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }} />
                  </label>

                  <label style={{ display: "grid", gap: 4 }}>
                    <div style={{ fontWeight: 800 }}>Адрес (необязательно)</div>
                    <input value={infoAddress} onChange={(e) => setInfoAddress(e.target.value)} style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }} />
                  </label>
                </div>

                {/* <-- добавили: выбор количества фото электро */}
                <label style={{ display: "grid", gap: 4 }}>
                  <div style={{ fontWeight: 800 }}>Электро: сколько фото ждём (сколько столбцов показывать)</div>
                  <select
                    value={infoElectricExpected}
                    onChange={(e) => setInfoElectricExpected(e.target.value)}
                    style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd", maxWidth: 220 }}
                  >
                    <option value="1">1 (T1)</option>
                    <option value="2">2 (T1, T2)</option>
                    <option value="3">3 (T1, T2, T3)</option>
                  </select>
                </label>

                <div style={{ display: "grid", gap: 8, gridTemplateColumns: "1fr 1fr" }}>
                  <label style={{ display: "grid", gap: 4 }}>
                    <div style={{ fontWeight: 800 }}>Жилец: имя (для отображения)</div>
                    <input value={infoTenantName} onChange={(e) => setInfoTenantName(e.target.value)} style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }} />
                  </label>

                  <label style={{ display: "grid", gap: 4 }}>
                    <div style={{ fontWeight: 800 }}>Комментарий (необязательно)</div>
                    <input value={infoNote} onChange={(e) => setInfoNote(e.target.value)} style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }} />
                  </label>
                </div>

                <div style={{ display: "grid", gap: 8, gridTemplateColumns: "1fr 1fr" }}>
                  <label style={{ display: "grid", gap: 4 }}>
                    <div style={{ fontWeight: 800 }}>Дата заселения</div>
                    <input
                      type="date"
                      value={infoTenantSince}
                      onChange={(e) => setInfoTenantSince(e.target.value)}
                      style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }}
                    />
                  </label>

                  <label style={{ display: "grid", gap: 4 }}>
                    <div style={{ fontWeight: 800 }}>Сумма арендной платы (₽)</div>
                    <input
                      value={infoRentMonthly}
                      onChange={(e) => setInfoRentMonthly(e.target.value)}
                      placeholder="0"
                      style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }}
                    />
                  </label>
                </div>

                <div className="info-section" style={{ paddingTop: 8 }}>
                  <div style={{ fontWeight: 900, marginBottom: 8 }}>Оплата счетчиков (режим)</div>
                  <div style={{ display: "grid", gap: 8, gridTemplateColumns: "1fr 1fr" }}>
                    <label style={{ display: "grid", gap: 4 }}>
                      <div style={{ fontWeight: 800 }}>Режим оплаты счетчиков</div>
                      <select
                        value={infoUtilitiesMode}
                        onChange={(e) => setInfoUtilitiesMode(e.target.value as any)}
                        style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }}
                      >
                        <option value="by_actual_monthly">По факту ежемесячно</option>
                        <option value="fixed_monthly">Фикс ежемесячно</option>
                        <option value="quarterly_advance">Аванс за N месяцев</option>
                      </select>
                    </label>
                    <label style={{ display: "grid", gap: 4 }}>
                      <div style={{ fontWeight: 800 }}>Показывать факт клиенту</div>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, height: 38 }}>
                        <input
                          type="checkbox"
                          checked={infoUtilitiesShowActualToTenant}
                          onChange={(e) => setInfoUtilitiesShowActualToTenant(e.target.checked)}
                        />
                        <span style={{ color: "#666", fontSize: 12 }}>Включено = клиент видит расчет по факту</span>
                      </div>
                    </label>
                  </div>

                  {infoUtilitiesMode === "fixed_monthly" ? (
                    <div style={{ display: "grid", gap: 8, gridTemplateColumns: "1fr 1fr", marginTop: 8 }}>
                      <label style={{ display: "grid", gap: 4 }}>
                        <div style={{ fontWeight: 800 }}>Фикс в месяц (₽)</div>
                        <input
                          value={infoUtilitiesFixedMonthly}
                          onChange={(e) => setInfoUtilitiesFixedMonthly(e.target.value)}
                          placeholder="3000"
                          style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd", maxWidth: 220 }}
                        />
                      </label>
                    </div>
                  ) : null}

                  {infoUtilitiesMode === "quarterly_advance" ? (
                    <div style={{ display: "grid", gap: 8, gridTemplateColumns: "1fr 1fr 1fr", marginTop: 8 }}>
                      <label style={{ display: "grid", gap: 4 }}>
                        <div style={{ fontWeight: 800 }}>Сумма аванса (₽)</div>
                        <input
                          value={infoUtilitiesAdvanceAmount}
                          onChange={(e) => setInfoUtilitiesAdvanceAmount(e.target.value)}
                          placeholder="9000"
                          style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }}
                        />
                      </label>
                      <label style={{ display: "grid", gap: 4 }}>
                        <div style={{ fontWeight: 800 }}>Длина цикла (мес)</div>
                        <input
                          value={infoUtilitiesAdvanceCycleMonths}
                          onChange={(e) => setInfoUtilitiesAdvanceCycleMonths(e.target.value)}
                          placeholder="3"
                          style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }}
                        />
                      </label>
                      <label style={{ display: "grid", gap: 4 }}>
                        <div style={{ fontWeight: 800 }}>Старт цикла (YYYY-MM)</div>
                        <input
                          value={infoUtilitiesAdvanceAnchorYm}
                          onChange={(e) => setInfoUtilitiesAdvanceAnchorYm(e.target.value)}
                          placeholder="2026-02"
                          style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }}
                        />
                      </label>
                    </div>
                  ) : null}
                </div>

                <div className="info-section" style={{ paddingTop: 8 }}>
                  <div style={{ fontWeight: 900, marginBottom: 8 }}>Серийные номера счётчиков</div>

                  <div style={{ display: "grid", gap: 8, gridTemplateColumns: "1fr 1fr" }}>
                    <label style={{ display: "grid", gap: 4 }}>
                      <div style={{ fontWeight: 800 }}>ХВС серийный номер</div>
                      <input
                        value={infoColdSerial}
                        onChange={(e) => setInfoColdSerial(e.target.value)}
                        placeholder="Только цифры и тире"
                        style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }}
                      />
                    </label>

                    <label style={{ display: "grid", gap: 4 }}>
                      <div style={{ fontWeight: 800 }}>ГВС серийный номер</div>
                      <input
                        value={infoHotSerial}
                        onChange={(e) => setInfoHotSerial(e.target.value)}
                        placeholder="Только цифры и тире"
                        style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }}
                      />
                    </label>
                  </div>
                </div>

                <div className="info-section" style={{ paddingTop: 8 }}>
                  <div style={{ fontWeight: 900, marginBottom: 8 }}>Контакты для авто-привязки фото</div>

                  <div style={{ display: "grid", gap: 8, gridTemplateColumns: "1fr 1fr" }}>
                    <label style={{ display: "grid", gap: 4 }}>
                      <div style={{ fontWeight: 800 }}>Телефон</div>
                      <input
                        value={infoPhone}
                        onChange={(e) => setInfoPhone(e.target.value)}
                        placeholder="+7 999 111-22-33 или 8 999 111-22-33"
                        style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }}
                      />
                    </label>

                    <label style={{ display: "grid", gap: 4 }}>
                      <div style={{ fontWeight: 800 }}>Telegram username</div>
                      <input value={infoTelegram} onChange={(e) => setInfoTelegram(e.target.value)} placeholder="@username" style={{ padding: 8, borderRadius: 10, border: "1px solid #ddd" }} />
                    </label>
                  </div>
                </div>

                <div className="info-section" style={{ paddingTop: 8 }}>
                  <div style={{ fontWeight: 900, marginBottom: 8 }}>Привязанные Telegram ID (chat_id)</div>

                  {!infoChats.length ? (
                    <div style={{ color: "#666" }}>Пока нет привязок. Они появятся автоматически после первого совпадения по телефону/нику или после ручной привязки ниже.</div>
                  ) : (
                    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                      {infoChats.map((c) => (
                        <div
                          className="info-chat-item"
                          key={c.chat_id}
                          style={{
                            display: "flex",
                            justifyContent: "space-between",
                            gap: 10,
                            alignItems: "center",
                            padding: 10,
                            border: "1px solid #eee",
                            borderRadius: 10,
                          }}
                        >
                          <div>
                            <div style={{ fontWeight: 900 }}>{c.chat_id}</div>
                            <div style={{ color: "#666", fontSize: 12 }}>
                              {c.is_active ? "active" : "inactive"}
                            </div>
                          </div>
                          <button
                            className="action-btn"
                            onClick={() => unbindChat(c.chat_id, selected.id)}
                            style={{ padding: "8px 10px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 900 }}
                          >
                            Отвязать
                          </button>
                        </div>
                      ))}
                    </div>
                  )}

                  <div style={{ marginTop: 10, display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
                    <input
                      value={bindChatInput}
                      onChange={(e) => setBindChatInput(e.target.value)}
                      placeholder="Ввести chat_id для ручной привязки"
                      style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd", minWidth: 260, flex: "1 1 260px" }}
                    />
                    <button className="action-btn is-primary" onClick={() => bindChatToApartment(selected.id)} style={{ padding: "10px 12px", borderRadius: 10, border: "1px solid #111", background: "#111", color: "white", cursor: "pointer", fontWeight: 900 }}>
                      Привязать
                    </button>
                  </div>
                </div>

                <div className="info-footer" style={{ display: "flex", gap: 10, justifyContent: "flex-end", paddingTop: 8, marginTop: 8 }}>
                  <button className="action-btn" onClick={() => setInfoOpen(false)} style={{ padding: "10px 12px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 900 }}>
                    Отмена
                  </button>
                  <button className="action-btn is-primary" onClick={() => saveInfo(selected.id)} style={{ padding: "10px 12px", borderRadius: 10, border: "1px solid #111", background: "#111", color: "white", cursor: "pointer", fontWeight: 900 }}>
                    Сохранить
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Add apartment modal */}
      {addOpen && (
        <div
          onClick={() => setAddOpen(false)}
          className="modal-overlay"
        >
          <div onClick={(e) => e.stopPropagation()} className="modal-shell add-modal" style={{ width: 520, maxWidth: "100%" }}>
            <div className="add-modal-title" style={{ fontWeight: 900, marginBottom: 10, fontSize: 18 }}>Добавить квартиру</div>

            <div style={{ display: "grid", gap: 10 }}>
              <label style={{ display: "grid", gap: 6 }}>
                <div style={{ fontWeight: 800 }}>Название *</div>
                <input
                  value={newTitle}
                  onChange={(e) => setNewTitle(e.target.value)}
                  placeholder="Например: Квартира 1"
                  style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }}
                />
              </label>

              <label style={{ display: "grid", gap: 6 }}>
                <div style={{ fontWeight: 800 }}>Адрес (необязательно)</div>
                <input
                  value={newAddress}
                  onChange={(e) => setNewAddress(e.target.value)}
                  placeholder="Улица, дом, квартира"
                  style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }}
                />
              </label>

              <div style={{ display: "flex", gap: 10, justifyContent: "flex-end", marginTop: 4 }}>
                <button
                  className="action-btn"
                  onClick={() => setAddOpen(false)}
                  style={{
                    padding: "10px 12px",
                    borderRadius: 10,
                    border: "1px solid #ddd",
                    background: "white",
                    cursor: "pointer",
                    fontWeight: 900,
                  }}
                >
                  Отмена
                </button>
                <button
                  className="action-btn is-primary"
                  onClick={createApartment}
                  style={{
                    padding: "10px 12px",
                    borderRadius: 10,
                    border: "1px solid #111",
                    background: "#111",
                    color: "white",
                    cursor: "pointer",
                    fontWeight: 900,
                  }}
                >
                  Сохранить
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>

      {/* Global tariffs modal */}
      {globalTariffsOpen && (
        <div
          onClick={() => setGlobalTariffsOpen(false)}
          className="modal-overlay"
        >
          <div onClick={(e) => e.stopPropagation()} className="modal-shell tariff-modal" style={{ width: 920, maxWidth: "100%", maxHeight: "92vh", overflow: "auto" }}>
            <div className="tariff-modal-head">
              <div style={{ fontWeight: 900, fontSize: 18 }}>Тарифы (по умолчанию)</div>
              <div className="tariff-modal-actions">
                <button
                  onClick={() => loadTariffs()}
                  style={{ padding: "8px 10px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 900 }}
                >
                  Обновить
                </button>
                <button
                  onClick={() => setGlobalTariffsOpen(false)}
                  style={{ padding: "8px 10px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 900 }}
                >
                  Закрыть
                </button>
              </div>
            </div>

            <div style={{ marginTop: 12, display: "grid", gap: 10 }}>
              <div className="tariff-form-grid-global">
                <input
                  placeholder="С месяца (YYYY-MM)"
                  value={tariffYmFrom}
                  onChange={(e) => setTariffYmFrom(e.target.value)}
                  style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }}
                />

                <input placeholder="ХВС" value={tariffCold} onChange={(e) => setTariffCold(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                <input placeholder="ГВС" value={tariffHot} onChange={(e) => setTariffHot(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />

                <input placeholder="Эл. T1" value={tariffElectricT1} onChange={(e) => setTariffElectricT1(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                <input placeholder="Эл. T2" value={tariffElectricT2} onChange={(e) => setTariffElectricT2(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />

                <input placeholder="Водоотв" value={tariffSewer} onChange={(e) => setTariffSewer(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />

                <button onClick={saveTariff} className="tariff-save-btn" style={{ padding: "10px 12px", borderRadius: 10, border: "1px solid #111", background: "#111", color: "white", cursor: "pointer", fontWeight: 900 }}>
                  Сохранить
                </button>
              </div>

              <div style={{ color: "#666", fontSize: 13 }}>
                Подсказка: значения — “цена за единицу”. Электро: тарифицируем по T1 и T2.
              </div>

              <div className="tariff-table-wrap">
                {loadingTariffs ? (
                  <div style={{ color: "#666" }}>Загрузка...</div>
                ) : !tariffs.length ? (
                  <div style={{ color: "#666" }}>Тарифов пока нет</div>
                ) : (
                  <table style={{ width: "100%", borderCollapse: "collapse" }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>С месяца</th>
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>ХВС</th>
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>ГВС</th>
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Эл. T1</th>
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Эл. T2</th>
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Водоотв</th>
                      </tr>
                    </thead>
                    <tbody>
                      {tariffs.map((t, i) => {
                        const baseE = t.electric ?? 0;
                        const e1 = (t.electric_t1 ?? baseE) as number;
                        const e2 = (t.electric_t2 ?? baseE) as number;
                        return (
                          <tr key={i}>
                            <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{ymDisplay(t.ym_from)}</td>
                            <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.cold}</td>
                            <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.hot}</td>
                            <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{e1}</td>
                            <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{e2}</td>
                            <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.sewer}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Apartment tariffs modal */}
      {apTariffsOpen && selected && (
        <div
          onClick={() => setApTariffsOpen(false)}
          className="modal-overlay"
          onMouseMove={(e) => {
            if (!apTariffsDrag.active) return;
            setApTariffsPos({ x: e.clientX - apTariffsDrag.dx, y: e.clientY - apTariffsDrag.dy });
          }}
          onMouseUp={() => setApTariffsDrag({ active: false, dx: 0, dy: 0 })}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            className="modal-shell tariff-modal"
            onMouseDown={(e) => {
              const t = e.target as HTMLElement;
              if (t.closest("input,select,textarea,button,label,a,[role='button']")) return;
              setApTariffsDrag({ active: true, dx: e.clientX - apTariffsPos.x, dy: e.clientY - apTariffsPos.y });
            }}
            style={{
              width: 920,
              maxWidth: "100%",
              maxHeight: "92vh",
              overflow: "auto",
              transform: `translate(${apTariffsPos.x}px, ${apTariffsPos.y}px)`,
              cursor: apTariffsDrag.active ? "grabbing" : "grab",
            }}
          >
            <div className="tariff-modal-head">
              <div className="panel-title" style={{ margin: 0 }}>Тарифы квартиры: {selected.title}</div>
              <div className="tariff-modal-actions">
                <button
                  onClick={() => loadApartmentTariffs(selected.id)}
                  className="action-btn"
                >
                  Обновить
                </button>
                <button
                  onClick={() => setApTariffsOpen(false)}
                  className="action-btn"
                >
                  Закрыть
                </button>
              </div>
            </div>

            <div style={{ marginTop: 10, color: "#666", fontSize: 13 }}>
              Здесь задаём тарифы только для этой квартиры. Пустое поле = не задавать (квартира наследует базовый тариф).
            </div>

            <form
              style={{ marginTop: 12, display: "grid", gap: 10 }}
              onSubmit={(e) => {
                e.preventDefault();
                void saveApartmentTariff(selected.id);
              }}
            >
              <div style={{ color: "var(--muted)", fontSize: 12 }}>
                Если месяц не заполнен, используем текущий: {normalizeYmAny(serverYm) ? ymDisplay(String(serverYm)) : "—"}.
              </div>

              <div className="tariff-form-grid-global">
                <input
                  placeholder="С месяца (YYYY-MM)"
                  value={apTariffYmFrom}
                  onChange={(e) => setApTariffYmFrom(e.target.value)}
                  onBlur={(e) => {
                    const ym = normalizeYmAny(e.target.value);
                    if (ym) setApTariffYmFrom(ym);
                  }}
                  style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }}
                />

                <input placeholder="ХВС" value={apTariffCold} onChange={(e) => setApTariffCold(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                <input placeholder="ГВС" value={apTariffHot} onChange={(e) => setApTariffHot(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />

                <input placeholder="Эл. T1" value={apTariffElectricT1} onChange={(e) => setApTariffElectricT1(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                <input placeholder="Эл. T2" value={apTariffElectricT2} onChange={(e) => setApTariffElectricT2(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                <input placeholder="Водоотв" value={apTariffSewer} onChange={(e) => setApTariffSewer(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />

                <button type="submit" className="tariff-save-btn action-btn is-primary">
                  Сохранить
                </button>
              </div>

              <div className="tariff-effective-block" style={{ borderRadius: 12, padding: 12 }}>
                <div style={{ fontWeight: 500, fontSize: 13 }}>Эффективный тариф (для выбранного месяца)</div>
                <div className="tariff-effective-grid" style={{ marginTop: 8, display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr 1fr", gap: 10 }}>
                  {(() => {
                    const ym = (historyWithFuture?.[historyWithFuture.length - 1]?.month ?? "").trim();
                    const eff = effectiveTariffForMonthForSelected(ym);
                    return (
                      <>
                        <div>ХВС: {fmtNum(eff.cold, 3)}</div>
                        <div>ГВС: {fmtNum(eff.hot, 3)}</div>
                        <div>Эл T1: {fmtNum(eff.e1, 3)}</div>
                        <div>Эл T2: {fmtNum(eff.e2, 3)}</div>
                        <div>Водоотв: {fmtNum(eff.sewer, 3)}</div>
                        <div style={{ gridColumn: "1 / -1", color: "#666", fontSize: 12 }}>
                          Источник: {eff.source === "apartment" ? "квартира" : eff.source === "global" ? "база" : "нет"}; применяем с: {eff.ym_from ? ymDisplay(String(eff.ym_from)) : "—"}
                        </div>
                      </>
                    );
                  })()}
                </div>
              </div>

              <div className="tariff-table-wrap">
                {loadingApTariffs ? (
                  <div style={{ color: "#666" }}>Загрузка...</div>
                ) : !apTariffs.length ? (
                  <div style={{ color: "#666" }}>Переопределений пока нет</div>
                ) : (
                  <table style={{ width: "100%", borderCollapse: "collapse" }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>С месяца</th>
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>ХВС</th>
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>ГВС</th>
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Эл. T1</th>
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Эл. T2</th>
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Водоотв</th>
                      </tr>
                    </thead>
                    <tbody>
                      {apTariffs.map((t, i) => (
                        <tr key={i}>
                          <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{ymDisplay(ymFromAny(t as any))}</td>
                          <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.cold ?? "—"}</td>
                          <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.hot ?? "—"}</td>
                          <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.electric_t1 ?? (t.electric ?? "—")}</td>
                          <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.electric_t2 ?? (t.electric ?? "—")}</td>
                          <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.sewer ?? "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </form>
          </div>
        </div>
      )}

      {settingsOpen && (
        <div
          onClick={() => setSettingsOpen(false)}
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.55)",
            backdropFilter: "blur(6px)",
            WebkitBackdropFilter: "blur(6px)",
            display: "grid",
            placeItems: "center",
            padding: 18,
            zIndex: 2000,
          }}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            style={{
              width: 560,
              maxWidth: "100%",
              borderRadius: 24,
              background: "var(--surface)",
              border: "1px solid var(--hair)",
              boxShadow: "var(--shadow)",
              padding: 14,
              color: "var(--text)",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12, padding: "4px 4px 10px", borderBottom: "1px solid var(--top-sep)", marginBottom: 12 }}>
              <div>
                <div style={{ fontWeight: 700 }}>Тема и индикаторы</div>
                <div style={{ fontSize: 12, color: "var(--muted)" }}>Светлая тёплая/светлая холодная/тёмная/ультра</div>
              </div>
              <button className="action-btn" onClick={() => setSettingsOpen(false)}>Закрыть</button>
            </div>

            <div style={{ display: "grid", gap: 12, padding: 4 }}>
              <div style={{ border: "1px solid var(--hair)", borderRadius: 18, padding: 12, background: "color-mix(in srgb, var(--panel2) 60%, transparent)" }}>
                <div style={{ marginBottom: 10, fontWeight: 700, fontSize: 13 }}>Тема</div>
                <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                  <label className="choice">
                    <input type="radio" checked={uiTheme === "light"} onChange={() => setUiTheme("light")} />
                    Светлая тёплая
                  </label>
                  <label className="choice">
                    <input type="radio" checked={uiTheme === "light-cool"} onChange={() => setUiTheme("light-cool")} />
                    Светлая холодная
                  </label>
                  <label className="choice">
                    <input type="radio" checked={uiTheme === "dark"} onChange={() => setUiTheme("dark")} />
                    Тёмная
                  </label>
                  <label className="choice">
                    <input type="radio" checked={uiTheme === "ultra"} onChange={() => setUiTheme("ultra")} />
                    Ультра тёмная
                  </label>
                </div>
              </div>

              <div style={{ border: "1px solid var(--hair)", borderRadius: 18, padding: 12, background: "color-mix(in srgb, var(--panel2) 60%, transparent)" }}>
                <div style={{ marginBottom: 10, fontWeight: 700, fontSize: 13 }}>Палитра индикаторов</div>
                <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                  <label className="choice">
                    <input type="radio" checked={indicatorPalette === "classic"} onChange={() => setIndicatorPalette("classic")} />
                    Палитра 1
                  </label>
                  <label className="choice">
                    <input type="radio" checked={indicatorPalette === "bright"} onChange={() => setIndicatorPalette("bright")} />
                    Палитра 2 (яркая)
                  </label>
                </div>
                <div style={{ margin: "12px 0 10px", fontWeight: 700, fontSize: 13 }}>Стиль индикаторов</div>
                <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                  <label className="choice">
                    <input type="radio" checked={indicatorStyle === "dot"} onChange={() => setIndicatorStyle("dot")} />
                    Круг
                  </label>
                  <label className="choice">
                    <input type="radio" checked={indicatorStyle === "diamond"} onChange={() => setIndicatorStyle("diamond")} />
                    Ромб
                  </label>
                  <label className="choice">
                    <input type="radio" checked={indicatorStyle === "triangles"} onChange={() => setIndicatorStyle("triangles")} />
                    Треугольники
                  </label>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

    </>
  );
}
