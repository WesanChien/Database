USE TAtest;
GO

CREATE OR ALTER PROCEDURE dbo.sp_DetectGranvilleRule2And6
    @StockCode VARCHAR(10),
    @ToleranceDays INT
AS
BEGIN
    SET NOCOUNT ON;

    IF @ToleranceDays < 1 OR @ToleranceDays > 5
    BEGIN
        RAISERROR(N'@ToleranceDays 需介於 1 到 5 之間', 16, 1);
        RETURN;
    END

    ;WITH Base AS
    (
        SELECT
            [Date],
            StockCode,
            [Close],
            MA20,
            Trend,
            LAG([Close], 1) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevClose1,
            LAG(MA20, 1) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevMA201,
            LAG([Close], 2) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevClose2,
            LAG(MA20, 2) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevMA202,
            LAG([Close], 3) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevClose3,
            LAG(MA20, 3) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevMA203,
            LAG([Close], 4) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevClose4,
            LAG(MA20, 4) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevMA204,
            LAG([Close], 5) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevClose5,
            LAG(MA20, 5) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevMA205,
            LAG([Close], 6) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevClose6,
            LAG(MA20, 6) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevMA206,
            LAG([Close], 7) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevClose7,
            LAG(MA20, 7) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PrevMA207
        FROM dbo.TW50_DailyPrice
        WHERE StockCode = @StockCode
          AND MA20 IS NOT NULL
          AND Trend IS NOT NULL
    )
    SELECT
        StockCode,
        [Date] AS SignalDate,
        CAST([Close] AS DECIMAL(10,2)) AS [Close],
        CAST(MA20 AS DECIMAL(10,2)) AS MA20,
        Trend,
        @ToleranceDays AS ToleranceDays,
        CASE
            WHEN
                Trend = N'上漲趨勢'
                AND [Close] > MA20
                AND PrevClose1 > PrevMA201
                AND (
                    (@ToleranceDays = 1
                        AND PrevClose2 < PrevMA202
                        AND PrevClose3 > PrevMA203)
                 OR (@ToleranceDays = 2
                        AND PrevClose2 < PrevMA202
                        AND PrevClose3 < PrevMA203
                        AND PrevClose4 > PrevMA204)
                 OR (@ToleranceDays = 3
                        AND PrevClose2 < PrevMA202
                        AND PrevClose3 < PrevMA203
                        AND PrevClose4 < PrevMA204
                        AND PrevClose5 > PrevMA205)
                 OR (@ToleranceDays = 4
                        AND PrevClose2 < PrevMA202
                        AND PrevClose3 < PrevMA203
                        AND PrevClose4 < PrevMA204
                        AND PrevClose5 < PrevMA205
                        AND PrevClose6 > PrevMA206)
                 OR (@ToleranceDays = 5
                        AND PrevClose2 < PrevMA202
                        AND PrevClose3 < PrevMA203
                        AND PrevClose4 < PrevMA204
                        AND PrevClose5 < PrevMA205
                        AND PrevClose6 < PrevMA206
                        AND PrevClose7 > PrevMA207)
                )
            THEN 'Buy_Granville2'

            WHEN
                Trend = N'下跌趨勢'
                AND [Close] < MA20
                AND PrevClose1 < PrevMA201
                AND (
                    (@ToleranceDays = 1
                        AND PrevClose2 > PrevMA202
                        AND PrevClose3 < PrevMA203)
                 OR (@ToleranceDays = 2
                        AND PrevClose2 > PrevMA202
                        AND PrevClose3 > PrevMA203
                        AND PrevClose4 < PrevMA204)
                 OR (@ToleranceDays = 3
                        AND PrevClose2 > PrevMA202
                        AND PrevClose3 > PrevMA203
                        AND PrevClose4 > PrevMA204
                        AND PrevClose5 < PrevMA205)
                 OR (@ToleranceDays = 4
                        AND PrevClose2 > PrevMA202
                        AND PrevClose3 > PrevMA203
                        AND PrevClose4 > PrevMA204
                        AND PrevClose5 > PrevMA205
                        AND PrevClose6 < PrevMA206)
                 OR (@ToleranceDays = 5
                        AND PrevClose2 > PrevMA202
                        AND PrevClose3 > PrevMA203
                        AND PrevClose4 > PrevMA204
                        AND PrevClose5 > PrevMA205
                        AND PrevClose6 > PrevMA206
                        AND PrevClose7 < PrevMA207)
                )
            THEN 'Sell_Granville6'
        END AS buy_or_sell
    FROM Base
    WHERE
        (
            Trend = N'上漲趨勢'
            AND [Close] > MA20
            AND PrevClose1 > PrevMA201
            AND (
                (@ToleranceDays = 1
                    AND PrevClose2 < PrevMA202
                    AND PrevClose3 > PrevMA203)
             OR (@ToleranceDays = 2
                    AND PrevClose2 < PrevMA202
                    AND PrevClose3 < PrevMA203
                    AND PrevClose4 > PrevMA204)
             OR (@ToleranceDays = 3
                    AND PrevClose2 < PrevMA202
                    AND PrevClose3 < PrevMA203
                    AND PrevClose4 < PrevMA204
                    AND PrevClose5 > PrevMA205)
             OR (@ToleranceDays = 4
                    AND PrevClose2 < PrevMA202
                    AND PrevClose3 < PrevMA203
                    AND PrevClose4 < PrevMA204
                    AND PrevClose5 < PrevMA205
                    AND PrevClose6 > PrevMA206)
             OR (@ToleranceDays = 5
                    AND PrevClose2 < PrevMA202
                    AND PrevClose3 < PrevMA203
                    AND PrevClose4 < PrevMA204
                    AND PrevClose5 < PrevMA205
                    AND PrevClose6 < PrevMA206
                    AND PrevClose7 > PrevMA207)
            )
        )
        OR
        (
            Trend = N'下跌趨勢'
            AND [Close] < MA20
            AND PrevClose1 < PrevMA201
            AND (
                (@ToleranceDays = 1
                    AND PrevClose2 > PrevMA202
                    AND PrevClose3 < PrevMA203)
             OR (@ToleranceDays = 2
                    AND PrevClose2 > PrevMA202
                    AND PrevClose3 > PrevMA203
                    AND PrevClose4 < PrevMA204)
             OR (@ToleranceDays = 3
                    AND PrevClose2 > PrevMA202
                    AND PrevClose3 > PrevMA203
                    AND PrevClose4 > PrevMA204
                    AND PrevClose5 < PrevMA205)
             OR (@ToleranceDays = 4
                    AND PrevClose2 > PrevMA202
                    AND PrevClose3 > PrevMA203
                    AND PrevClose4 > PrevMA204
                    AND PrevClose5 > PrevMA205
                    AND PrevClose6 < PrevMA206)
             OR (@ToleranceDays = 5
                    AND PrevClose2 > PrevMA202
                    AND PrevClose3 > PrevMA203
                    AND PrevClose4 > PrevMA204
                    AND PrevClose5 > PrevMA205
                    AND PrevClose6 > PrevMA206
                    AND PrevClose7 < PrevMA207)
            )
        )
    ORDER BY SignalDate DESC;
END
GO
