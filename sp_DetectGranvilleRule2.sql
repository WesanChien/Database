USE TAtest;
GO

CREATE OR ALTER PROCEDURE dbo.sp_DetectGranvilleRule2
    @StockCode VARCHAR(10),
    @ToleranceDays INT
AS
BEGIN
    SET NOCOUNT ON;

    IF @ToleranceDays < 1 OR @ToleranceDays > 6
    BEGIN
        RAISERROR(N'@ToleranceDays 需介於 1 到 6 之間', 16, 1);
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
            CASE WHEN [Close] > MA20 THEN 1 ELSE 0 END AS AboveMA20,
            CASE WHEN [Close] < MA20 THEN 1 ELSE 0 END AS BelowMA20,
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
    )
    SELECT
        StockCode,
        [Date] AS BuyDate,
        CAST([Close] AS DECIMAL(10,2)) AS [Close],
        CAST(MA20 AS DECIMAL(10,2)) AS MA20,
        Trend,
        @ToleranceDays AS ToleranceDays
    FROM Base
    WHERE
        Trend = N'上漲趨勢'
        AND [Close] > MA20                      -- 最後一天在均線上
        AND PrevClose1 > PrevMA201             -- 倒數第二天在均線上
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
    ORDER BY BuyDate DESC;
END
GO
