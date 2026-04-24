-- 建議先在 SSMS 執行這份 SQL
USE [TAtest];
GO

IF OBJECT_ID('dbo.TW50_Constituents_Current', 'U') IS NOT NULL
    DROP TABLE dbo.TW50_Constituents_Current;
GO

CREATE TABLE dbo.TW50_Constituents_Current (
    SnapshotDate DATE NOT NULL,
    StockCode VARCHAR(10) NOT NULL,
    StockName NVARCHAR(100) NULL,
    Source NVARCHAR(100) NULL,
    CONSTRAINT PK_TW50_Constituents_Current PRIMARY KEY (SnapshotDate, StockCode)
);
GO

IF OBJECT_ID('dbo.TW50_DailyPrice', 'U') IS NOT NULL
    DROP TABLE dbo.TW50_DailyPrice;
GO

CREATE TABLE dbo.TW50_DailyPrice (
    [Date] DATE NOT NULL,
    StockCode VARCHAR(10) NOT NULL,
    Capacity BIGINT NULL,              -- 成交股數
    [Volume] DECIMAL(20,2) NULL,       -- 成交金額
    [Open] DECIMAL(10,2) NULL,
    High DECIMAL(10,2) NULL,
    Low DECIMAL(10,2) NULL,
    [Close] DECIMAL(10,2) NULL,
    [Change] DECIMAL(10,2) NULL,
    [Transaction] BIGINT NULL,         -- 成交筆數
    MA5 DECIMAL(10,2) NULL,
    MA10 DECIMAL(10,2) NULL,
    MA20 DECIMAL(10,2) NULL,
    MA60 DECIMAL(10,2) NULL,
    MA120 DECIMAL(10,2) NULL,
    MA240 DECIMAL(10,2) NULL,
    CONSTRAINT PK_TW50_DailyPrice PRIMARY KEY ([Date], StockCode)
);
GO

CREATE INDEX IX_TW50_DailyPrice_StockCode_Date
ON dbo.TW50_DailyPrice (StockCode, [Date]);
GO
