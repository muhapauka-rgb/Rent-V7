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
};

export default function MetersTable(props: Props) {
  const {
    rows,
    eN,
    effectiveTariffForMonth,
    calcElectricT3Fallback,
    calcSewerDelta,
    calcSumRub,
    cellTriplet,
    fmtRub,
    openEdit,
    getReviewFlag,
    onResolveReviewFlag,
  } = props;

  const n = Math.max(1, Math.min(3, Number.isFinite(eN) ? eN : 3));

  return (
    <div style={{ marginTop: 12 }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Месяц</th>
            <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>ХВС</th>
            <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>ГВС</th>

            {n >= 1 && <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>T1</th>}
            {n >= 2 && <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>T2</th>}
            {n >= 3 && <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>T3</th>}

            <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Водоотв</th>
            <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Сумма</th>
            <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>Действия</th>
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

            // Если в квартире ожидается 3 электро-индекса — сумму НЕ показываем, пока не пришёл T3
            const isComplete =
              !missingCold &&
              !missingHot &&
              !missingE1 &&
              !missingE2 &&
              !missingE3;

            // Сумма = ХВС + ГВС + (T1 если показываем) + (T2 если показываем) + водоотведение
            const sum = isComplete ? calcSumRub(rc, rh, n >= 1 ? re1 : null, n >= 2 ? re2 : null, rs) : null;


            return (
              <tr key={h.month}>
                <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2", whiteSpace: "nowrap" }}>{h.month}</td>

                <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                  {cellTriplet(h.meters?.cold?.current ?? null, dc, rc, t.cold, true, fCold ? "review" : (missingCold ? "missing" : "none"))}
                  {fCold ? (
                    <div style={{ marginTop: 6 }}>
                      <button onClick={() => onResolveReviewFlag(fCold.id)} style={{ background: "#fee2e2", color: "#991b1b", border: "1px solid #fca5a5", borderRadius: 999, padding: "2px 8px", fontSize: 11, fontWeight: 700, cursor: "pointer" }}>
                        Проверить значение
                      </button>
                    </div>
                  ) : null}
                </td>

                <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                  {cellTriplet(h.meters?.hot?.current ?? null, dh, rh, t.hot, true, fHot ? "review" : (missingHot ? "missing" : "none"))}
                  {fHot ? (
                    <div style={{ marginTop: 6 }}>
                      <button onClick={() => onResolveReviewFlag(fHot.id)} style={{ background: "#fee2e2", color: "#991b1b", border: "1px solid #fca5a5", borderRadius: 999, padding: "2px 8px", fontSize: 11, fontWeight: 700, cursor: "pointer" }}>
                        Проверить значение
                      </button>
                    </div>
                  ) : null}
                </td>

                {n >= 1 && (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                    {cellTriplet(h.meters?.electric?.t1?.current ?? null, de1, re1, t.e1, true, fE1 ? "review" : (missingE1 ? "missing" : "none"))}
                    {fE1 ? (
                      <div style={{ marginTop: 6 }}>
                        <button onClick={() => onResolveReviewFlag(fE1.id)} style={{ background: "#fee2e2", color: "#991b1b", border: "1px solid #fca5a5", borderRadius: 999, padding: "2px 8px", fontSize: 11, fontWeight: 700, cursor: "pointer" }}>
                          Проверить значение
                        </button>
                      </div>
                    ) : null}
                  </td>
                )}

                {n >= 2 && (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                    {cellTriplet(h.meters?.electric?.t2?.current ?? null, de2, re2, t.e2, true, fE2 ? "review" : (missingE2 ? "missing" : "none"))}
                    {fE2 ? (
                      <div style={{ marginTop: 6 }}>
                        <button onClick={() => onResolveReviewFlag(fE2.id)} style={{ background: "#fee2e2", color: "#991b1b", border: "1px solid #fca5a5", borderRadius: 999, padding: "2px 8px", fontSize: 11, fontWeight: 700, cursor: "pointer" }}>
                          Проверить значение
                        </button>
                      </div>
                    ) : null}
                  </td>
                )}

                {n >= 3 && (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                    {cellTriplet(t3fb.current, de3, null, null, false, fE3 ? "review" : (missingE3 ? "missing" : "none"))}
                    {fE3 ? (
                      <div style={{ marginTop: 6 }}>
                        <button onClick={() => onResolveReviewFlag(fE3.id)} style={{ background: "#fee2e2", color: "#991b1b", border: "1px solid #fca5a5", borderRadius: 999, padding: "2px 8px", fontSize: 11, fontWeight: 700, cursor: "pointer" }}>
                          Проверить значение
                        </button>
                      </div>
                    ) : null}
                  </td>
                )}

                <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                  {cellTriplet(h.meters?.sewer?.current ?? null, ds, rs, t.sewer, true, fSewer ? "review" : "none")}
                  {fSewer ? (
                    <div style={{ marginTop: 6 }}>
                      <button onClick={() => onResolveReviewFlag(fSewer.id)} style={{ background: "#fee2e2", color: "#991b1b", border: "1px solid #fca5a5", borderRadius: 999, padding: "2px 8px", fontSize: 11, fontWeight: 700, cursor: "pointer" }}>
                        Проверить значение
                      </button>
                    </div>
                  ) : null}
                </td>

                <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2", whiteSpace: "nowrap", fontWeight: 900 }}>
                  {sum == null ? "—" : `₽ ${fmtRub(sum)}`}
                </td>

                <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                  <button
                    onClick={() => openEdit(h.month)}
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
                    Редактировать
                  </button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>

      <div style={{ marginTop: 8, color: "#666", fontSize: 12 }}>
        Пояснение: ₽ = Δ × тариф месяца. Водоотведение: если sewer.delta пустой — считаем как Δ(ХВС)+Δ(ГВС). Электро: тарифицируем только T1 и T2. T3 — без тарифа (инфо).
        Оранжевым выделены значения, по которым ещё ждём фото от клиента.
        Красным и меткой "Проверить значение" отмечены жалобы клиента на неверное распознавание.
      </div>
    </div>
  );
}
