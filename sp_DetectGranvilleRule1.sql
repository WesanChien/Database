USE TAtest;
GO

CREATE OR ALTER PROCEDURE dbo.sp_DetectGranvilleRule1
    @StockCode VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH Base AS
    (
        SELECT
            [Date],
            StockCode,
            [Close],
            MA20,
            Trend,
            LAG(Trend) OVER (PARTITION BY StockCode ORDER BY [Date]) AS PreviousTrend
        FROM dbo.TW50_DailyPrice
        WHERE StockCode = @StockCode
          AND MA20 IS NOT NULL
    ),
    Flagged AS
    (
        SELECT
            [Date],
            StockCode,
            [Close],
            MA20,
            Trend,
            PreviousTrend,
            CASE WHEN [Close] < MA20 THEN 1 ELSE 0 END AS BelowMA20Flag
        FROM Base
    ),
    Windowed AS
    (
        SELECT
            [Date],
            StockCode,
            [Close],
            MA20,
            Trend,
            PreviousTrend,
            SUM(BelowMA20Flag) OVER (
                PARTITION BY StockCode
                ORDER BY [Date]
                ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING
            ) AS Prev8DaysBelowCount
        FROM Flagged
    )
    SELECT
        StockCode,
        [Date] AS BuyDate,
        CAST([Close] AS DECIMAL(10,2)) AS [Close],
        CAST(MA20 AS DECIMAL(10,2)) AS MA20,
        PreviousTrend,
        Trend AS CurrentTrend
    FROM Windowed
    WHERE [Close] > MA20
      AND Prev8DaysBelowCount >= 6
      AND (
            (PreviousTrend = N'下跌趨勢' AND Trend = N'盤整整理')
         OR (PreviousTrend = N'盤整整理' AND Trend = N'上漲趨勢')
      )
    ORDER BY BuyDate DESC;
END
GO
