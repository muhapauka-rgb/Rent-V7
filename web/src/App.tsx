import React, { useEffect, useMemo, useState } from "react";
import MetersTable from "./components/MetersTable";

type ApartmentItem = {
  id: number;
  title: string;
  address?: string | null;
  electric_expected?: number | null;
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

type ApartmentCardResp = {
  ok: boolean;
  apartment: {
    id: number;
    title: string;
    address?: string | null;
    tenant_name?: string | null;
    note?: string | null;
    electric_expected?: number | null; // <-- добавили (может приходить, а может нет)
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

export default function App() {
  const [tab, setTab] = useState<"apartments" | "ops">("apartments");
  const [err, setErr] = useState<string | null>(null);

  const [apartments, setApartments] = useState<ApartmentItem[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);

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

  // <-- добавили: сколько фото электро ждём (1..3)
  const [infoElectricExpected, setInfoElectricExpected] = useState<string>("1");

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
      setInfoChats(data.chats ?? []);
      setBindChatInput("");

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
  const [apTariffElectricT3, setApTariffElectricT3] = useState("");

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
    ym_from: string | null;
  } {
    const m = (month || "").trim();
    if (!isYm(m) || !apTariffs.length) {
      return { cold: null, hot: null, e1: null, e2: null, e3: null, sewer: null, ym_from: null };
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
      ym_from: ymFromAny(best as any) || null,
    };
  }

  function effectiveTariffForMonthForSelected(month: string): {
    cold: number;
    hot: number;
    e1: number;
    e2: number;
    sewer: number;
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
      statuses: {
        all_photos_received: Boolean((x as any)?.statuses?.all_photos_received),
        meters_photo: Boolean((x as any)?.statuses?.meters_photo),
        rent_paid: Boolean((x as any)?.statuses?.rent_paid),
        meters_paid: Boolean((x as any)?.statuses?.meters_paid),
      },
    }));

    setApartments(items);

    if (selectIfEmpty) {
      setSelectedId((prev) => {
        if (prev != null) return prev;
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
    const ym = tariffYmFrom.trim();
    if (!ym) {
      setErr("Заполни 'С месяца' (YYYY-MM).");
      return;
    }

    const e1 = numOrZero(tariffElectricT1);
    const e2 = numOrZero(tariffElectricT2);

    // T3 тарифа нет. Но чтобы не ломать БД-совместимость — пишем electric_t3 как e1+e2 (информативно).
    const e3_info = e1 + e2;

    const payload = {
      ym_from: ym,
      cold: numOrZero(tariffCold),
      hot: numOrZero(tariffHot),
      sewer: numOrZero(tariffSewer),

      electric: e1, // совместимость
      electric_t1: e1,
      electric_t2: e2,
      electric_t3: e3_info,
    };

    try {
      setErr(null);
      await apiPost("/tariffs", payload);
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
    const ym = apTariffYmFrom.trim();
    if (!isYm(ym)) {
      setErr("Формат месяца: YYYY-MM");
      return;
    }

    const payload: any = { month_from: ym };

    const cold = numOrNull(apTariffCold);
    const hot = numOrNull(apTariffHot);
    const sewer = numOrNull(apTariffSewer);
    const e1 = numOrNull(apTariffElectricT1);
    const e2 = numOrNull(apTariffElectricT2);
    const e3 = numOrNull(apTariffElectricT3);

    if (cold !== null) payload.cold = cold;
    if (hot !== null) payload.hot = hot;
    if (sewer !== null) payload.sewer = sewer;

    // электро: допускаем пустые (не задавать) — тогда наследуем базовые
    if (e1 !== null) payload.electric_t1 = e1;
    if (e2 !== null) payload.electric_t2 = e2;
    if (e3 !== null) payload.electric_t3 = e3;

    try {
      setErr(null);
      await apiPost(`/admin/ui/apartments/${apartmentId}/tariffs`, payload);
      await loadApartmentTariffs(apartmentId);

      // очистим форму (не трогаем month_from — удобно для серии правок)
      setApTariffCold("");
      setApTariffHot("");
      setApTariffSewer("");
      setApTariffElectricT1("");
      setApTariffElectricT2("");
      setApTariffElectricT3("");
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

  const latest = historyWithFuture.length ? historyWithFuture[historyWithFuture.length - 1] : null;
  const latestMonth = latest?.month ?? null;
  const latestMeters = latest?.meters ?? null;

  const last4 = historyWithFuture.slice(-4).reverse();
  // сколько столбцов электро показывать (T1/T2/T3)
  const eN = Math.max(1, Math.min(3, Number((selected as any)?.electric_expected ?? 1) || 1));

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
    const color = highlightMode === "review" ? "#b91c1c" : highlightMode === "missing" ? "#d97706" : "#111";
    return (
      <div style={{ display: "grid", gap: 2, lineHeight: 1.25 }}>
        <div style={{ fontWeight: 900, color }}>{fmtNum(current, 3)}</div>
        <div style={{ color: "#666", fontSize: 12 }}>Δ {fmtNum(delta, 3)}</div>
        <div style={{ color: "#111", fontSize: 12, fontWeight: 800 }}>{rubEnabled ? (rub == null ? "₽ —" : `₽ ${fmtRub(rub)}`) : "₽ —"}</div>
        <div style={{ color: "#777", fontSize: 11 }}>тариф: {tariff == null ? "—" : fmtNum(tariff, 3)}</div>
      </div>
    );
  }

  function calcSumRub(rc: number | null, rh: number | null, re1: number | null, re2: number | null, rs: number | null) {
    const parts = [rc, rh, re1, re2, rs].filter((x) => x != null && Number.isFinite(x)) as number[];
    if (!parts.length) return null;
    return parts.reduce((a, b) => a + b, 0);
  }

  // Для карточек вверху (последний месяц)
  const latestTariff = useMemo(() => (latestMonth ? effectiveTariffForMonth(latestMonth) : null), [latestMonth, tariffs]);
  const latestRowComputed = useMemo(() => {
    if (!latest || !latestTariff) return { sum: null };
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

    const sum = isComplete ? calcSumRub(rc, rh, re1, re2, rs) : null;
    return { sum };

  }, [latest, latestTariff]);

  function renderStatusSwitch(
    checked: boolean,
    onToggle?: () => void,
    readOnly: boolean = false
  ) {
    const bg = checked ? "#0A84FF" : "#C9972B";
    return (
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          if (!readOnly && onToggle) onToggle();
        }}
        style={{
          width: 42,
          height: 24,
          borderRadius: 999,
          border: "none",
          padding: 2,
          background: bg,
          display: "flex",
          alignItems: "center",
          justifyContent: checked ? "flex-end" : "flex-start",
          cursor: readOnly ? "default" : "pointer",
          opacity: readOnly ? 0.9 : 1,
          transition: "all 160ms ease",
        }}
      >
        <span
          style={{
            width: 20,
            height: 20,
            borderRadius: "50%",
            background: "white",
            boxShadow: "0 1px 3px rgba(0,0,0,0.25)",
          }}
        />
      </button>
    );
  }

  return (
    <>
    <div style={{ fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, Arial", padding: 24 }}>
      <div style={{ display: "flex", gap: 10, alignItems: "center", justifyContent: "space-between" }}>
        <h1 style={{ margin: 0 }}>Rent Web</h1>

        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <button
            onClick={() => setTab("apartments")}
            style={{
              padding: "8px 12px",
              borderRadius: 10,
              border: tab === "apartments" ? "2px solid #111" : "1px solid #ddd",
              background: tab === "apartments" ? "#f7f7f7" : "white",
              cursor: "pointer",
              fontWeight: 800,
            }}
          >
            Квартиры
          </button>

          <button
            onClick={() => setTab("ops")}
            style={{
              padding: "8px 12px",
              borderRadius: 10,
              border: tab === "ops" ? "2px solid #111" : "1px solid #ddd",
              background: tab === "ops" ? "#f7f7f7" : "white",
              cursor: "pointer",
              fontWeight: 800,
            }}
          >
            Операции
          </button>

          <button
            onClick={() => setAddOpen(true)}
            style={{
              padding: "8px 12px",
              borderRadius: 10,
              border: "1px solid #111",
              background: "#111",
              color: "white",
              cursor: "pointer",
              fontWeight: 900,
            }}
          >
            + Квартира
          </button>

          <button
            onClick={() => deleteSelectedApartment()}
            style={{
              padding: "8px 12px",
              borderRadius: 10,
              border: "1px solid #ddd",
              background: "white",
              cursor: "pointer",
              fontWeight: 900,
            }}
          >
            Удалить
          </button>
        </div>
      </div>

      {err ? (
        <div style={{ marginTop: 12, background: "#fff2f2", border: "1px solid #ffd0d0", color: "#8a0000", padding: 12, borderRadius: 12 }}>
          <div style={{ fontWeight: 900 }}>Ошибка</div>
          <div style={{ marginTop: 6, whiteSpace: "pre-wrap" }}>{err}</div>
        </div>
      ) : null}

      {tab === "apartments" ? (
        <div style={{ marginTop: 16, display: "grid", gridTemplateColumns: "320px 1fr", gap: 16, alignItems: "start" }}>
          {/* LEFT */}
          <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
            <div style={{ fontWeight: 900, marginBottom: 10 }}>Квартиры</div>

            {!apartments.length ? (
              <div style={{ color: "#666" }}>Пока нет квартир. Нажми “+ Квартира”.</div>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {apartments.map((a) => {
                  const active = a.id === selectedId;
                  return (
                    <button
                      key={a.id}
                      onClick={() => setSelectedId(a.id)}
                      style={{
                        textAlign: "left",
                        padding: 10,
                        borderRadius: 10,
                        border: active ? "2px solid #111" : "1px solid #eee",
                        background: active ? "#f7f7f7" : "white",
                        cursor: "pointer",
                      }}
                    >
                      <div style={{ fontWeight: 900 }}>{a.title}</div>
                      {a.address ? <div style={{ fontSize: 12, color: "#666" }}>{a.address}</div> : null}
                      <div style={{ marginTop: 8, display: "grid", gap: 6 }}>
                        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                          <div style={{ fontSize: 12, color: "#444" }}>Фото счетчиков</div>
                          {renderStatusSwitch(
                            Boolean(a.statuses?.meters_photo),
                            () => togglePaidStatus(a.id, "meters_photo", Boolean(a.statuses?.meters_photo)),
                            false
                          )}
                        </div>
                        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                          <div style={{ fontSize: 12, color: "#444" }}>Оплата аренды</div>
                          {renderStatusSwitch(
                            Boolean(a.statuses?.rent_paid),
                            () => togglePaidStatus(a.id, "rent_paid", Boolean(a.statuses?.rent_paid)),
                            false
                          )}
                        </div>
                        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                          <div style={{ fontSize: 12, color: "#444" }}>Оплата счетчиков</div>
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
          <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
            <div style={{ fontWeight: 900, marginBottom: 8 }}>Показания (последние 4 месяца)</div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
              <div style={{ color: "#666", fontSize: 13 }}>
                В каждой ячейке: текущее / Δ / ₽ / тариф. Следующий месяц добавлен автоматически — можно сразу редактировать.
              </div>
              
              <div style={{ display: "flex", gap: 8 }}>
                <button
                  disabled={!selected}
                  onClick={() => {
                    if (!selected) return;
                    setApTariffsOpen(true);
                    loadApartmentTariffs(selected.id).catch(() => {});
                  }}
                  style={{
                    padding: "8px 10px",
                    borderRadius: 10,
                    border: "1px solid #ddd",
                    background: "white",
                    cursor: selected ? "pointer" : "not-allowed",
                    fontWeight: 800,
                    opacity: selected ? 1 : 0.5,
                  }}
                >
                  Тарифы
                </button>

                <button
                  disabled={!selected}
                  onClick={() => selected && openInfo(selected.id)}
                  style={{
                    padding: "8px 10px",
                    borderRadius: 10,
                    border: "1px solid #ddd",
                    background: "white",
                    cursor: selected ? "pointer" : "not-allowed",
                    fontWeight: 800,
                    opacity: selected ? 1 : 0.5,
                  }}
                >
                  Инфо
                </button>
              </div>
            </div>

            {!selected ? (
              <div style={{ color: "#666" }}>Выбери квартиру слева или создай новую.</div>
            ) : loadingHistory ? (
              <div style={{ color: "#666" }}>Загрузка...</div>
            ) : !historyWithFuture.length ? (
              <div style={{ color: "#666" }}>Пока нет показаний по этой квартире.</div>
            ) : (
              <>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10 }}>
                  <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 10 }}>
                    <div style={{ fontWeight: 800 }}>Месяц</div>
                    <div style={{ fontSize: 18 }}>{latestMonth}</div>
                  </div>

                  <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 10 }}>
                    <div style={{ fontWeight: 800 }}>ХВС</div>
                    <div style={{ fontSize: 18 }}>{fmt(latestMeters?.cold?.current)}</div>
                  </div>

                  <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 10 }}>
                    <div style={{ fontWeight: 800 }}>ГВС</div>
                    <div style={{ fontSize: 18 }}>{fmt(latestMeters?.hot?.current)}</div>
                  </div>

                  <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 10 }}>
                    <div style={{ fontWeight: 800 }}>Сумма (₽)</div>
                    <div style={{ fontSize: 18, fontWeight: 900 }}>{latestRowComputed.sum == null ? "—" : `₽ ${fmtRub(latestRowComputed.sum)}`}</div>
                    <div style={{ color: "#777", fontSize: 11 }}>Считаем: ХВС + ГВС + T1 + T2 + Водоотв</div>
                  </div>
                </div>

                <div style={{ marginTop: 12 }}>
                  <MetersTable
                    rows={last4}
                    eN={eN}
                    effectiveTariffForMonth={(m) => effectiveTariffForMonthForSelected(m)}
                    calcSewerDelta={calcSewerDelta}
                    calcElectricT3Fallback={calcElectricT3Fallback}
                    cellTriplet={cellTriplet}
                    calcSumRub={calcSumRub}
                    fmtRub={fmtRub}
                    openEdit={openEdit}
                    getReviewFlag={getReviewFlag}
                    onResolveReviewFlag={resolveReviewFlag}
                  />

                  <div style={{ marginTop: 8, color: "#666", fontSize: 12 }}>
                    Пояснение: ₽ = Δ × тариф месяца. Водоотведение: если sewer.delta пустой — считаем как Δ(ХВС)+Δ(ГВС). Электро: тарифицируем только T1 и T2. T3 (итого) — без тарифа (инфо).
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      ) : (
        <div style={{ marginTop: 16, display: "grid", gridTemplateColumns: "1fr", gap: 16 }}>

          {/* Tariffs (global) */}
          <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12 }}>
              <div>
                <div style={{ fontWeight: 900 }}>Тарифы (по умолчанию)</div>
                <div style={{ marginTop: 6, color: "#666", fontSize: 13 }}>
                  Базовые тарифы применяются ко всем квартирам, если у квартиры нет переопределения.
                </div>
              </div>
              <button
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
          <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div style={{ fontWeight: 900 }}>Неразобранные фото</div>
              <button onClick={() => loadUnassigned()} style={{ padding: "8px 12px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 800 }}>
                Обновить
              </button>
            </div>

            <div style={{ marginTop: 10, display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
              <div style={{ fontWeight: 800 }}>Куда назначать:</div>
              <select
                value={assignApartmentId}
                onChange={(e) => setAssignApartmentId(e.target.value ? Number(e.target.value) : "")}
                style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd", minWidth: 220 }}
              >
                <option value="">— выбери квартиру —</option>
                {apartments.map((a) => (
                  <option key={a.id} value={a.id}>
                    {a.title}
                  </option>
                ))}
              </select>

              <label style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <input type="checkbox" checked={bindChatId} onChange={(e) => setBindChatId(e.target.checked)} />
                <span style={{ fontWeight: 700 }}>Привязать chat_id (если найден)</span>
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
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.35)",
            display: "grid",
            placeItems: "center",
            padding: 16,
          }}
        >
          <div onClick={(e) => e.stopPropagation()} style={{ width: 640, maxWidth: "100%", background: "white", borderRadius: 14, padding: 14 }}>
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

      {/* Apartment info modal */}
      {infoOpen && selected && (
        <div
          onClick={() => setInfoOpen(false)}
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.35)",
            display: "grid",
            placeItems: "center",
            padding: 16,
          }}
        >
          <div onClick={(e) => e.stopPropagation()} style={{ width: 640, maxWidth: "100%", background: "white", borderRadius: 14, padding: 14 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 10 }}>
              <div style={{ fontWeight: 900, fontSize: 18 }}>Карточка квартиры</div>
              <button onClick={() => setInfoOpen(false)} style={{ padding: "8px 10px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 900 }}>
                Закрыть
              </button>
            </div>

            {infoLoading ? (
              <div style={{ marginTop: 12, color: "#666" }}>Загрузка...</div>
            ) : (
              <div style={{ marginTop: 12, display: "grid", gap: 12 }}>
                <div style={{ display: "grid", gap: 10, gridTemplateColumns: "1fr 1fr" }}>
                  <label style={{ display: "grid", gap: 6 }}>
                    <div style={{ fontWeight: 800 }}>Название квартиры *</div>
                    <input value={infoTitle} onChange={(e) => setInfoTitle(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                  </label>

                  <label style={{ display: "grid", gap: 6 }}>
                    <div style={{ fontWeight: 800 }}>Адрес (необязательно)</div>
                    <input value={infoAddress} onChange={(e) => setInfoAddress(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                  </label>
                </div>

                {/* <-- добавили: выбор количества фото электро */}
                <label style={{ display: "grid", gap: 6 }}>
                  <div style={{ fontWeight: 800 }}>Электро: сколько фото ждём (сколько столбцов показывать)</div>
                  <select
                    value={infoElectricExpected}
                    onChange={(e) => setInfoElectricExpected(e.target.value)}
                    style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd", maxWidth: 220 }}
                  >
                    <option value="1">1 (T1)</option>
                    <option value="2">2 (T1, T2)</option>
                    <option value="3">3 (T1, T2, T3)</option>
                  </select>
                  <div style={{ color: "#666", fontSize: 12 }}>
                    Это влияет на интерфейс (сколько колонок электро показываем) и на то, сколько фото электро ожидаем от жильца.
                  </div>
                </label>

                <div style={{ display: "grid", gap: 10, gridTemplateColumns: "1fr 1fr" }}>
                  <label style={{ display: "grid", gap: 6 }}>
                    <div style={{ fontWeight: 800 }}>Жилец: имя (для отображения)</div>
                    <input value={infoTenantName} onChange={(e) => setInfoTenantName(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                  </label>

                  <label style={{ display: "grid", gap: 6 }}>
                    <div style={{ fontWeight: 800 }}>Комментарий (необязательно)</div>
                    <input value={infoNote} onChange={(e) => setInfoNote(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                  </label>
                </div>

                <div style={{ borderTop: "1px solid #eee", paddingTop: 12 }}>
                  <div style={{ fontWeight: 900, marginBottom: 8 }}>Контакты для авто-привязки фото</div>

                  <div style={{ display: "grid", gap: 10, gridTemplateColumns: "1fr 1fr" }}>
                    <label style={{ display: "grid", gap: 6 }}>
                      <div style={{ fontWeight: 800 }}>Телефон</div>
                      <input
                        value={infoPhone}
                        onChange={(e) => setInfoPhone(e.target.value)}
                        placeholder="+7 999 111-22-33 или 8 999 111-22-33"
                        style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }}
                      />
                      <div style={{ color: "#666", fontSize: 12 }}>Можно в любом формате: пробелы, +7, 8 — сервер сам нормализует.</div>
                    </label>

                    <label style={{ display: "grid", gap: 6 }}>
                      <div style={{ fontWeight: 800 }}>Telegram username</div>
                      <input value={infoTelegram} onChange={(e) => setInfoTelegram(e.target.value)} placeholder="@username" style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                      <div style={{ color: "#666", fontSize: 12 }}>Можно с @ или без — сервер сам нормализует.</div>
                    </label>
                  </div>
                </div>

                <div style={{ borderTop: "1px solid #eee", paddingTop: 12 }}>
                  <div style={{ fontWeight: 900, marginBottom: 8 }}>Привязанные Telegram ID (chat_id)</div>

                  {!infoChats.length ? (
                    <div style={{ color: "#666" }}>Пока нет привязок. Они появятся автоматически после первого совпадения по телефону/нику или после ручной привязки ниже.</div>
                  ) : (
                    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                      {infoChats.map((c) => (
                        <div
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
                              {c.is_active ? "active" : "inactive"}; updated {c.updated_at}
                            </div>
                          </div>
                          <button
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
                    <button onClick={() => bindChatToApartment(selected.id)} style={{ padding: "10px 12px", borderRadius: 10, border: "1px solid #111", background: "#111", color: "white", cursor: "pointer", fontWeight: 900 }}>
                      Привязать
                    </button>
                  </div>
                  <div style={{ color: "#666", fontSize: 12, marginTop: 6 }}>
                    Обычно ручная привязка не нужна: если у жильца совпадёт телефон или username — привязка создастся сама при первом фото.
                  </div>
                </div>

                <div style={{ display: "flex", gap: 10, justifyContent: "flex-end", position: "sticky", bottom: 0, background: "white", paddingTop: 12, marginTop: 12, borderTop: "1px solid #eee" }}>
                  <button onClick={() => setInfoOpen(false)} style={{ padding: "10px 12px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 900 }}>
                    Отмена
                  </button>
                  <button onClick={() => saveInfo(selected.id)} style={{ padding: "10px 12px", borderRadius: 10, border: "1px solid #111", background: "#111", color: "white", cursor: "pointer", fontWeight: 900 }}>
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
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.35)",
            display: "grid",
            placeItems: "center",
            padding: 16,
          }}
        >
          <div onClick={(e) => e.stopPropagation()} style={{ width: 520, maxWidth: "100%", background: "white", borderRadius: 14, padding: 14 }}>
            <div style={{ fontWeight: 900, marginBottom: 10, fontSize: 18 }}>Добавить квартиру</div>

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
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.35)",
            display: "grid",
            placeItems: "center",
            padding: 16,
          }}
        >
          <div onClick={(e) => e.stopPropagation()} style={{ width: 920, maxWidth: "100%", background: "white", borderRadius: 14, padding: 14 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 10 }}>
              <div style={{ fontWeight: 900, fontSize: 18 }}>Тарифы (по умолчанию)</div>
              <div style={{ display: "flex", gap: 8 }}>
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
              <div style={{ display: "grid", gridTemplateColumns: "200px 1fr 1fr 1fr 1fr 1fr 140px", gap: 8, alignItems: "center" }}>
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

                <button onClick={saveTariff} style={{ padding: "10px 12px", borderRadius: 10, border: "1px solid #111", background: "#111", color: "white", cursor: "pointer", fontWeight: 900 }}>
                  Сохранить
                </button>
              </div>

              <div style={{ color: "#666", fontSize: 13 }}>
                Подсказка: значения — “цена за единицу”. Электро: тарифицируем только T1 и T2. T3 — без тарифа (итого/инфо).
              </div>

              <div>
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
                            <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.ym_from}</td>
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
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.35)",
            display: "grid",
            placeItems: "center",
            padding: 16,
          }}
        >
          <div onClick={(e) => e.stopPropagation()} style={{ width: 920, maxWidth: "100%", background: "white", borderRadius: 14, padding: 14 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 10 }}>
              <div style={{ fontWeight: 900, fontSize: 18 }}>Тарифы квартиры: {selected.title}</div>
              <div style={{ display: "flex", gap: 8 }}>
                <button
                  onClick={() => loadApartmentTariffs(selected.id)}
                  style={{ padding: "8px 10px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 900 }}
                >
                  Обновить
                </button>
                <button
                  onClick={() => setApTariffsOpen(false)}
                  style={{ padding: "8px 10px", borderRadius: 10, border: "1px solid #ddd", background: "white", cursor: "pointer", fontWeight: 900 }}
                >
                  Закрыть
                </button>
              </div>
            </div>

            <div style={{ marginTop: 10, color: "#666", fontSize: 13 }}>
              Здесь задаём тарифы только для этой квартиры. Пустое поле = не задавать (квартира наследует базовый тариф).
            </div>

            <div style={{ marginTop: 12, display: "grid", gap: 10 }}>
              <div style={{ display: "grid", gridTemplateColumns: "200px 1fr 1fr 1fr 1fr 1fr 1fr 140px", gap: 8, alignItems: "center" }}>
                <input
                  placeholder="С месяца (YYYY-MM)"
                  value={apTariffYmFrom}
                  onChange={(e) => setApTariffYmFrom(e.target.value)}
                  style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }}
                />

                <input placeholder="ХВС" value={apTariffCold} onChange={(e) => setApTariffCold(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                <input placeholder="ГВС" value={apTariffHot} onChange={(e) => setApTariffHot(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />

                <input placeholder="Эл. T1" value={apTariffElectricT1} onChange={(e) => setApTariffElectricT1(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                <input placeholder="Эл. T2" value={apTariffElectricT2} onChange={(e) => setApTariffElectricT2(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />
                <input placeholder="Эл. T3 (инфо)" value={apTariffElectricT3} onChange={(e) => setApTariffElectricT3(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />

                <input placeholder="Водоотв" value={apTariffSewer} onChange={(e) => setApTariffSewer(e.target.value)} style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }} />

                <button
                  onClick={() => saveApartmentTariff(selected.id)}
                  style={{ padding: "10px 12px", borderRadius: 10, border: "1px solid #111", background: "#111", color: "white", cursor: "pointer", fontWeight: 900 }}
                >
                  Сохранить
                </button>
              </div>

              <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
                <div style={{ fontWeight: 900 }}>Эффективный тариф (для выбранного месяца)</div>
                <div style={{ marginTop: 8, display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr 1fr", gap: 10 }}>
                  {(() => {
                    const ym = (historyWithFuture?.[historyWithFuture.length - 1]?.month ?? "").trim();
                    const eff = effectiveTariffForMonthForSelected(ym);
                    return (
                      <>
                        <div>ХВС: <b>{fmtNum(eff.cold, 3)}</b></div>
                        <div>ГВС: <b>{fmtNum(eff.hot, 3)}</b></div>
                        <div>Эл T1: <b>{fmtNum(eff.e1, 3)}</b></div>
                        <div>Эл T2: <b>{fmtNum(eff.e2, 3)}</b></div>
                        <div>Водоотв: <b>{fmtNum(eff.sewer, 3)}</b></div>
                        <div style={{ gridColumn: "1 / -1", color: "#666", fontSize: 12 }}>
                          Источник: {eff.source === "apartment" ? "квартира" : eff.source === "global" ? "база" : "нет"}; применяем с: {eff.ym_from ?? "—"}
                        </div>
                      </>
                    );
                  })()}
                </div>
              </div>

              <div>
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
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Эл. T3</th>
                        <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Водоотв</th>
                      </tr>
                    </thead>
                    <tbody>
                      {apTariffs.map((t, i) => (
                        <tr key={i}>
                          <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{ymFromAny(t as any)}</td>
                          <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.cold ?? "—"}</td>
                          <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.hot ?? "—"}</td>
                          <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.electric_t1 ?? (t.electric ?? "—")}</td>
                          <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.electric_t2 ?? (t.electric ?? "—")}</td>
                          <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.electric_t3 ?? "—"}</td>
                          <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>{t.sewer ?? "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

    </>
  );
}
