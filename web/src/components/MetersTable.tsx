import React from "react";

type MeterCell = { title: string; current: number | null; previous: number | null; delta: number | null; source?: string | null };

type HistoryRow = {
  month: string;
  meters: {
    cold: MeterCell;
    hot: MeterCell;
    electric: {
      title: string;
      t1: MeterCell;
      t2: MeterCell;
      t3: MeterCell;
    };
    sewer: MeterCell;
  };
};

type Props = {
  rows: HistoryRow[];
  eN: number;
  currentYm?: string;
  showDualSummary?: boolean;
  showRentColumn?: boolean;
  rentForMonth?: (ym: string) => number;
  showPolicyColumns?: boolean;
  utilitiesForMonth?: (ym: string) => { actual: number | null; planned: number | null; carry: number | null } | null;

  effectiveTariffForMonth: (ym: string) => any;

  calcElectricT3Fallback: (h: HistoryRow) => { current: number | null; delta: number | null };
  calcSewerDelta: (h: HistoryRow) => number | null;
  calcSumRub: (...vals: Array<number | null>) => number | null;

  cellTriplet: (
    current: number | null,
    delta: number | null,
    rub: number | null,
    tariff: number | null,
    rubEnabled: boolean,
    highlightMode?: "none" | "missing" | "review"
  ) => React.ReactNode;

  fmtRub: (n: number | null | undefined) => string;
  openEdit: (month: string) => void;
  getReviewFlag: (month: string, meterType: string, meterIndex: number) => { id: number } | null;
  onResolveReviewFlag: (flagId: number) => void;
  notificationHighlight?: { ym: string; meter_type: string; meter_index: number } | null;
  onCellPhoto?: (month: string, meterType: string, meterIndex: number, flagId?: number) => void;
  onInlineSave?: (month: string, meterType: string, meterIndex: number, value: number) => Promise<void>;
};

export default function MetersTable(props: Props) {
  const {
    rows,
    eN,
    currentYm,
    showDualSummary = false,
    showRentColumn = false,
    rentForMonth,
    showPolicyColumns = false,
    utilitiesForMonth,
    effectiveTariffForMonth,
    calcElectricT3Fallback,
    calcSewerDelta,
    calcSumRub,
    cellTriplet,
    fmtRub,
    openEdit,
    getReviewFlag,
    onResolveReviewFlag,
    notificationHighlight,
    onCellPhoto,
    onInlineSave,
  } = props;

  const n = Math.max(1, Math.min(3, Number.isFinite(eN) ? eN : 3));
  const summaryCols = showDualSummary ? 2 : 1;
  const baseCols = n + 4; // month + cold + hot + sewer + electric(Tn)
  const totalCols = baseCols + (showRentColumn ? 1 : 0) + (showPolicyColumns ? 3 : 0) + summaryCols;
  const equalColWidth = `calc((100% - 56px) / ${totalCols})`;

  function ymRuLabel(ym: string): string {
    if (!/^\d{4}-\d{2}$/.test(ym || "")) return ym;
    const months = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"];
    const y = Number(ym.slice(0, 4));
    const m = Number(ym.slice(5, 7)) - 1;
    if (!Number.isFinite(y) || m < 0 || m > 11) return ym;
    return `${months[m]} ${y}`;
  }

  const [inlineKey, setInlineKey] = React.useState<string | null>(null);
  const [inlineValue, setInlineValue] = React.useState<string>("");
  const [inlineSaving, setInlineSaving] = React.useState(false);

  function keyOf(month: string, meterType: string, meterIndex: number) {
    return `${month}|${meterType}|${meterIndex}`;
  }

  function startInlineEdit(month: string, meterType: string, meterIndex: number, current: number | null) {
    if (!onInlineSave) return;
    setInlineKey(keyOf(month, meterType, meterIndex));
    setInlineValue(current == null ? "" : String(current));
  }

  function parseInlineNumber(s: string): number | null {
    const t = String(s ?? "").trim();
    if (!t) return 0; // empty => zero
    const v = Number(t.replace(",", "."));
    if (!Number.isFinite(v)) return null;
    return v;
  }

  async function commitInline(month: string, meterType: string, meterIndex: number) {
    if (!onInlineSave) return;
    const val = parseInlineNumber(inlineValue);
    if (val == null) return;
    setInlineSaving(true);
    try {
      await onInlineSave(month, meterType, meterIndex, val);
      setInlineKey(null);
      setInlineValue("");
    } finally {
      setInlineSaving(false);
    }
  }

  return (
    <div className="table-wrap-react">
      <table className="table canvas-table" style={{ width: "100%", borderCollapse: "collapse", tableLayout: "fixed" }}>
        <colgroup>
          <col style={{ width: equalColWidth }} />
          <col style={{ width: equalColWidth }} />
          <col style={{ width: equalColWidth }} />
          <col style={{ width: equalColWidth }} />
          {n >= 1 && <col style={{ width: equalColWidth }} />}
          {n >= 2 && <col style={{ width: equalColWidth }} />}
          {n >= 3 && <col style={{ width: equalColWidth }} />}
          {showRentColumn && <col style={{ width: equalColWidth }} />}
          {showPolicyColumns && <col style={{ width: equalColWidth }} />}
          {showPolicyColumns && <col style={{ width: equalColWidth }} />}
          {showPolicyColumns && <col style={{ width: equalColWidth }} />}
          {showDualSummary ? (
            <>
              <col style={{ width: equalColWidth }} />
              <col style={{ width: equalColWidth }} />
            </>
          ) : (
            <col style={{ width: equalColWidth }} />
          )}
          <col style={{ width: 56 }} />
        </colgroup>
        <thead>
          <tr>
            <th style={{ textAlign: "left", padding: "8px 8px 8px 12px", borderBottom: "1px solid #eee" }}>Месяц</th>
            <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>ХВС</th>
            <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>ГВС</th>
            <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Водоотв</th>

            {n >= 1 && <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>T1</th>}
            {n >= 2 && <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>T2</th>}
            {n >= 3 && <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>T3</th>}
            {showRentColumn && <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Аренда</th>}
            {showPolicyColumns && <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Начислено факт</th>}
            {showPolicyColumns && <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>К оплате</th>}
            {showPolicyColumns && <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Баланс</th>}
            {showDualSummary ? (
              <>
                <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Факт</th>
                <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>К оплате</th>
              </>
            ) : (
              <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Сумма</th>
            )}
            <th style={{ textAlign: "right", padding: "8px 0", borderBottom: "1px solid #eee", width: 56 }}></th>
          </tr>
        </thead>

        <tbody>
          {rows.map((h) => {
            const t = effectiveTariffForMonth(h.month);

            const dc = h.meters?.cold?.delta ?? null;
            const dh = h.meters?.hot?.delta ?? null;

            const de1 = h.meters?.electric?.t1?.delta ?? null;
            const de2 = h.meters?.electric?.t2?.delta ?? null;

            const t3fb = calcElectricT3Fallback(h);
            const de3 = t3fb.delta;

            const ds = calcSewerDelta(h);

            const rc = dc == null ? null : dc * (t.cold || 0);
            const rh = dh == null ? null : dh * (t.hot || 0);

            const re1 = de1 == null ? null : de1 * (t.e1 || 0);
            const re2 = de2 == null ? null : de2 * (t.e2 || 0);

            // ВАЖНО: T3 в рублях НЕ считаем
            const rs = ds == null ? null : ds * (t.sewer || 0);

            const ccur = h.meters?.cold?.current ?? null;
            const hcur = h.meters?.hot?.current ?? null;
            const e1cur = h.meters?.electric?.t1?.current ?? null;
            const e2cur = h.meters?.electric?.t2?.current ?? null;
            const e3cur = h.meters?.electric?.t3?.current ?? null;
            const e3src = String(h.meters?.electric?.t3?.source ?? "").toLowerCase();

            const missingCold = ccur == null;
            const missingHot = hcur == null;
            const missingE1 = n >= 1 && e1cur == null;
            const missingE2 = n >= 2 && e2cur == null;
            const missingE3 = n >= 3 && (e3cur == null || e3src !== "ocr");

            const fCold = getReviewFlag(h.month, "cold", 1);
            const fHot = getReviewFlag(h.month, "hot", 1);
            const fE1 = getReviewFlag(h.month, "electric", 1);
            const fE2 = getReviewFlag(h.month, "electric", 2);
            const fE3 = getReviewFlag(h.month, "electric", 3);
            const fSewer = getReviewFlag(h.month, "sewer", 1);

            const nCold = notificationHighlight && notificationHighlight.ym === h.month && notificationHighlight.meter_type === "cold" && Number(notificationHighlight.meter_index || 1) === 1;
            const nHot = notificationHighlight && notificationHighlight.ym === h.month && notificationHighlight.meter_type === "hot" && Number(notificationHighlight.meter_index || 1) === 1;
            const nE1 = notificationHighlight && notificationHighlight.ym === h.month && notificationHighlight.meter_type === "electric" && Number(notificationHighlight.meter_index || 1) === 1;
            const nE2 = notificationHighlight && notificationHighlight.ym === h.month && notificationHighlight.meter_type === "electric" && Number(notificationHighlight.meter_index || 1) === 2;
            const nE3 = notificationHighlight && notificationHighlight.ym === h.month && notificationHighlight.meter_type === "electric" && Number(notificationHighlight.meter_index || 1) === 3;
            const nSewer = notificationHighlight && notificationHighlight.ym === h.month && notificationHighlight.meter_type === "sewer" && Number(notificationHighlight.meter_index || 1) === 1;
            const reviewBtnStyle: React.CSSProperties = {
              background: "#fee2e2",
              color: "#991b1b",
              border: "1px solid #fca5a5",
              borderRadius: 999,
              padding: "2px 8px",
              fontSize: 11,
              fontWeight: 700,
              cursor: "pointer",
            };
            const resolveBtnStyle: React.CSSProperties = {
              background: "transparent",
              color: "var(--text)",
              border: "1px solid var(--hair)",
              borderRadius: 999,
              padding: "2px 8px",
              fontSize: 11,
              fontWeight: 700,
              cursor: "pointer",
            };

            // Если в квартире ожидается 3 электро-индекса — сумму НЕ показываем, пока не пришёл T3
            const isComplete =
              !missingCold &&
              !missingHot &&
              !missingE1 &&
              !missingE2 &&
              !missingE3;

            // Сумма = ХВС + ГВС + (T1 если показываем) + (T2 если показываем) + водоотведение
            const sumFact = isComplete ? calcSumRub(rc, rh, n >= 1 ? re1 : null, n >= 2 ? re2 : null, rs) : null;
            const isCurrent = (currentYm || "").trim() !== "" && h.month === currentYm;
            const rent = showRentColumn && rentForMonth ? rentForMonth(h.month) : 0;
            const utilities = showPolicyColumns && utilitiesForMonth ? utilitiesForMonth(h.month) : null;
            const sumPlanned = (utilitiesForMonth ? (utilitiesForMonth(h.month)?.planned ?? null) : null) ?? sumFact;


            return (
              <tr key={h.month} className={isCurrent ? "is-current" : ""}>
                <td style={{ padding: "8px 8px 8px 12px", borderBottom: "1px solid #f2f2f2", whiteSpace: "nowrap" }}>{ymRuLabel(h.month)}</td>

                <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                  {inlineKey === keyOf(h.month, "cold", 1) ? (
                    <div style={{ display: "grid", gap: 6 }}>
                      <input
                        autoFocus
                        value={inlineValue}
                        onChange={(e) => setInlineValue(e.target.value)}
                        onKeyDown={(e) => {
                          if (e.key === "Enter") commitInline(h.month, "cold", 1);
                          if (e.key === "Escape") setInlineKey(null);
                        }}
                        style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid var(--hair)" }}
                      />
                      <div style={{ display: "flex", gap: 6 }}>
                        <button onClick={() => commitInline(h.month, "cold", 1)} style={resolveBtnStyle} disabled={inlineSaving}>Сохранить</button>
                        <button onClick={() => setInlineKey(null)} style={resolveBtnStyle} disabled={inlineSaving}>Отмена</button>
                      </div>
                    </div>
                  ) : (
                    <div
                      onClick={() => onCellPhoto && onCellPhoto(h.month, "cold", 1)}
                      onDoubleClick={() => startInlineEdit(h.month, "cold", 1, h.meters?.cold?.current ?? null)}
                      style={{ cursor: onCellPhoto ? "pointer" : "default", position: "relative" }}
                    >
                      {cellTriplet(h.meters?.cold?.current ?? null, dc, rc, t.cold, true, nCold ? "review" : (fCold ? "review" : (missingCold ? "missing" : "none")))}
                    </div>
                  )}
                  {fCold ? (
                    <div style={{ marginTop: 6, display: "flex", gap: 6, flexWrap: "wrap" }}>
                      <button onClick={() => onCellPhoto && onCellPhoto(h.month, "cold", 1, fCold.id)} style={reviewBtnStyle}>
                        Проверить
                      </button>
                      <button onClick={() => onResolveReviewFlag(fCold.id)} style={resolveBtnStyle}>
                        Подтвердить
                      </button>
                    </div>
                  ) : null}
                </td>

                <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                  {inlineKey === keyOf(h.month, "hot", 1) ? (
                    <div style={{ display: "grid", gap: 6 }}>
                      <input
                        autoFocus
                        value={inlineValue}
                        onChange={(e) => setInlineValue(e.target.value)}
                        onKeyDown={(e) => {
                          if (e.key === "Enter") commitInline(h.month, "hot", 1);
                          if (e.key === "Escape") setInlineKey(null);
                        }}
                        style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid var(--hair)" }}
                      />
                      <div style={{ display: "flex", gap: 6 }}>
                        <button onClick={() => commitInline(h.month, "hot", 1)} style={resolveBtnStyle} disabled={inlineSaving}>Сохранить</button>
                        <button onClick={() => setInlineKey(null)} style={resolveBtnStyle} disabled={inlineSaving}>Отмена</button>
                      </div>
                    </div>
                  ) : (
                    <div
                      onClick={() => onCellPhoto && onCellPhoto(h.month, "hot", 1)}
                      onDoubleClick={() => startInlineEdit(h.month, "hot", 1, h.meters?.hot?.current ?? null)}
                      style={{ cursor: onCellPhoto ? "pointer" : "default" }}
                    >
                      {cellTriplet(h.meters?.hot?.current ?? null, dh, rh, t.hot, true, nHot ? "review" : (fHot ? "review" : (missingHot ? "missing" : "none")))}
                    </div>
                  )}
                  {fHot ? (
                    <div style={{ marginTop: 6, display: "flex", gap: 6, flexWrap: "wrap" }}>
                      <button onClick={() => onCellPhoto && onCellPhoto(h.month, "hot", 1, fHot.id)} style={reviewBtnStyle}>
                        Проверить
                      </button>
                      <button onClick={() => onResolveReviewFlag(fHot.id)} style={resolveBtnStyle}>
                        Подтвердить
                      </button>
                    </div>
                  ) : null}
                </td>

                <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                  {cellTriplet(h.meters?.sewer?.current ?? null, ds, rs, t.sewer, true, nSewer ? "review" : (fSewer ? "review" : "none"))}
                  {fSewer ? (
                    <div style={{ marginTop: 6, display: "flex", gap: 6, flexWrap: "wrap" }}>
                      <button onClick={() => onCellPhoto && onCellPhoto(h.month, "sewer", 1, fSewer.id)} style={reviewBtnStyle}>
                        Проверить
                      </button>
                      <button onClick={() => onResolveReviewFlag(fSewer.id)} style={resolveBtnStyle}>
                        Подтвердить
                      </button>
                    </div>
                  ) : null}
                </td>

                {n >= 1 && (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                    {inlineKey === keyOf(h.month, "electric", 1) ? (
                      <div style={{ display: "grid", gap: 6 }}>
                        <input
                          autoFocus
                          value={inlineValue}
                          onChange={(e) => setInlineValue(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") commitInline(h.month, "electric", 1);
                            if (e.key === "Escape") setInlineKey(null);
                          }}
                          style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid var(--hair)" }}
                        />
                        <div style={{ display: "flex", gap: 6 }}>
                          <button onClick={() => commitInline(h.month, "electric", 1)} style={resolveBtnStyle} disabled={inlineSaving}>Сохранить</button>
                          <button onClick={() => setInlineKey(null)} style={resolveBtnStyle} disabled={inlineSaving}>Отмена</button>
                        </div>
                      </div>
                    ) : (
                      <div
                        onClick={() => onCellPhoto && onCellPhoto(h.month, "electric", 1)}
                        onDoubleClick={() => startInlineEdit(h.month, "electric", 1, h.meters?.electric?.t1?.current ?? null)}
                        style={{ cursor: onCellPhoto ? "pointer" : "default" }}
                      >
                        {cellTriplet(h.meters?.electric?.t1?.current ?? null, de1, re1, t.e1, true, nE1 ? "review" : (fE1 ? "review" : (missingE1 ? "missing" : "none")))}
                      </div>
                    )}
                    {fE1 ? (
                      <div style={{ marginTop: 6, display: "flex", gap: 6, flexWrap: "wrap" }}>
                        <button onClick={() => onCellPhoto && onCellPhoto(h.month, "electric", 1, fE1.id)} style={reviewBtnStyle}>
                          Проверить
                        </button>
                        <button onClick={() => onResolveReviewFlag(fE1.id)} style={resolveBtnStyle}>
                          Подтвердить
                        </button>
                      </div>
                    ) : null}
                  </td>
                )}

                {n >= 2 && (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                    {inlineKey === keyOf(h.month, "electric", 2) ? (
                      <div style={{ display: "grid", gap: 6 }}>
                        <input
                          autoFocus
                          value={inlineValue}
                          onChange={(e) => setInlineValue(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") commitInline(h.month, "electric", 2);
                            if (e.key === "Escape") setInlineKey(null);
                          }}
                          style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid var(--hair)" }}
                        />
                        <div style={{ display: "flex", gap: 6 }}>
                          <button onClick={() => commitInline(h.month, "electric", 2)} style={resolveBtnStyle} disabled={inlineSaving}>Сохранить</button>
                          <button onClick={() => setInlineKey(null)} style={resolveBtnStyle} disabled={inlineSaving}>Отмена</button>
                        </div>
                      </div>
                    ) : (
                      <div
                        onClick={() => onCellPhoto && onCellPhoto(h.month, "electric", 2)}
                        onDoubleClick={() => startInlineEdit(h.month, "electric", 2, h.meters?.electric?.t2?.current ?? null)}
                        style={{ cursor: onCellPhoto ? "pointer" : "default" }}
                      >
                        {cellTriplet(h.meters?.electric?.t2?.current ?? null, de2, re2, t.e2, true, nE2 ? "review" : (fE2 ? "review" : (missingE2 ? "missing" : "none")))}
                      </div>
                    )}
                    {fE2 ? (
                      <div style={{ marginTop: 6, display: "flex", gap: 6, flexWrap: "wrap" }}>
                        <button onClick={() => onCellPhoto && onCellPhoto(h.month, "electric", 2, fE2.id)} style={reviewBtnStyle}>
                          Проверить
                        </button>
                        <button onClick={() => onResolveReviewFlag(fE2.id)} style={resolveBtnStyle}>
                          Подтвердить
                        </button>
                      </div>
                    ) : null}
                  </td>
                )}

                {n >= 3 && (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                    {inlineKey === keyOf(h.month, "electric", 3) ? (
                      <div style={{ display: "grid", gap: 6 }}>
                        <input
                          autoFocus
                          value={inlineValue}
                          onChange={(e) => setInlineValue(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") commitInline(h.month, "electric", 3);
                            if (e.key === "Escape") setInlineKey(null);
                          }}
                          style={{ padding: "6px 8px", borderRadius: 8, border: "1px solid var(--hair)" }}
                        />
                        <div style={{ display: "flex", gap: 6 }}>
                          <button onClick={() => commitInline(h.month, "electric", 3)} style={resolveBtnStyle} disabled={inlineSaving}>Сохранить</button>
                          <button onClick={() => setInlineKey(null)} style={resolveBtnStyle} disabled={inlineSaving}>Отмена</button>
                        </div>
                      </div>
                    ) : (
                      <div
                        onClick={() => onCellPhoto && onCellPhoto(h.month, "electric", 3)}
                        onDoubleClick={() => startInlineEdit(h.month, "electric", 3, t3fb.current)}
                        style={{ cursor: onCellPhoto ? "pointer" : "default" }}
                      >
                        {cellTriplet(t3fb.current, de3, null, null, false, nE3 ? "review" : (fE3 ? "review" : (missingE3 ? "missing" : "none")))}
                      </div>
                    )}
                    {fE3 ? (
                      <div style={{ marginTop: 6, display: "flex", gap: 6, flexWrap: "wrap" }}>
                        <button onClick={() => onCellPhoto && onCellPhoto(h.month, "electric", 3, fE3.id)} style={reviewBtnStyle}>
                          Проверить
                        </button>
                        <button onClick={() => onResolveReviewFlag(fE3.id)} style={resolveBtnStyle}>
                          Подтвердить
                        </button>
                      </div>
                    ) : null}
                  </td>
                )}

                {showRentColumn && (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2", whiteSpace: "nowrap", fontWeight: 500 }}>
                    {rent > 0 ? fmtRub(rent) : "—"}
                  </td>
                )}
                {showPolicyColumns && (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2", whiteSpace: "nowrap", fontWeight: 500 }}>
                    {utilities?.actual == null ? "—" : `${fmtRub(utilities.actual)}`}
                  </td>
                )}
                {showPolicyColumns && (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2", whiteSpace: "nowrap", fontWeight: 500 }}>
                    {utilities?.planned == null ? "—" : `${fmtRub(utilities.planned)}`}
                  </td>
                )}
                {showPolicyColumns && (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2", whiteSpace: "nowrap", fontWeight: 500 }}>
                    {utilities?.carry == null ? "—" : `${fmtRub(utilities.carry)}`}
                  </td>
                )}

                {showDualSummary ? (
                  <>
                    <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2", whiteSpace: "nowrap", fontWeight: 500 }}>
                      {sumFact == null ? "—" : `${fmtRub(sumFact)}`}
                    </td>
                    <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2", whiteSpace: "nowrap", fontWeight: 500 }}>
                      {sumPlanned == null ? "—" : `${fmtRub(sumPlanned)}`}
                    </td>
                  </>
                ) : (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2", whiteSpace: "nowrap", fontWeight: 500 }}>
                    {sumFact == null ? "—" : `${fmtRub(sumFact)}`}
                  </td>
                )}

                <td style={{ padding: "8px 0", borderBottom: "1px solid #f2f2f2", width: 56, textAlign: "right" }}>
                  <button
                    onClick={() => openEdit(h.month)}
                    className="btn secondary table-edit-btn has-delayed-tooltip"
                    data-tooltip="Редактировать"
                    aria-label="Редактировать"
                    style={{
                      width: "100%",
                      padding: "10px 12px",
                      borderRadius: 14,
                      border: "1px solid transparent",
                      background: "transparent",
                      color: "var(--text)",
                      cursor: "pointer",
                      fontWeight: 500,
                    }}
                  >
                    <svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true">
                      <path d="M12 20h9" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                      <path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                  </button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>

    </div>
  );
}
