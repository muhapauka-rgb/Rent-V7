import React from "react";

type MeterCell = { title: string; current: number | null; previous: number | null; delta: number | null };

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
    rubEnabled: boolean
  ) => React.ReactNode;

  fmtRub: (n: number | null | undefined) => string;
  openEdit: (month: string) => void;
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
            {n >= 3 && <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #eee" }}>T3 (итого)</th>}

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

            // Сумма = ХВС + ГВС + (T1 если показываем) + (T2 если показываем) + водоотведение
            const sum = calcSumRub(rc, rh, n >= 1 ? re1 : null, n >= 2 ? re2 : null, rs);

            return (
              <tr key={h.month}>
                <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2", whiteSpace: "nowrap" }}>{h.month}</td>

                <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                  {cellTriplet(h.meters?.cold?.current ?? null, dc, rc, t.cold, true)}
                </td>

                <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                  {cellTriplet(h.meters?.hot?.current ?? null, dh, rh, t.hot, true)}
                </td>

                {n >= 1 && (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                    {cellTriplet(h.meters?.electric?.t1?.current ?? null, de1, re1, t.e1, true)}
                  </td>
                )}

                {n >= 2 && (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                    {cellTriplet(h.meters?.electric?.t2?.current ?? null, de2, re2, t.e2, true)}
                  </td>
                )}

                {n >= 3 && (
                  <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                    {cellTriplet(t3fb.current, de3, null, null, false)}
                  </td>
                )}

                <td style={{ padding: 8, borderBottom: "1px solid #f2f2f2" }}>
                  {cellTriplet(h.meters?.sewer?.current ?? null, ds, rs, t.sewer, true)}
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
        Пояснение: ₽ = Δ × тариф месяца. Водоотведение: если sewer.delta пустой — считаем как Δ(ХВС)+Δ(ГВС). Электро: тарифицируем только T1 и T2. T3 (итого) — без тарифа (инфо).
      </div>
    </div>
  );
}
