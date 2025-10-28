TABLE_SCHEMAS = {
    "FairValueGaps": """
        CREATE TABLE IF NOT EXISTS FairValueGaps (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Symbol TEXT NOT NULL,
            ActiveTime TEXT,              -- Timestamp or datetime (ISO 8601 string)
            FVGStart REAL,                -- Price level (start)
            FVGEnd REAL,                  -- Price level (end)
            Direction TEXT,               -- 'Bullish' / 'Bearish'
            FVGType TEXT,                 -- e.g. 'Standard', 'Extended', etc.
            TimeFrame TEXT,               -- e.g. '1m', '5m', '1h', '1D'
            Duration INTEGER,             -- Duration in candles or minutes
            GapSize REAL,                 -- Percentage value (e.g. 1.25 for 1.25%)
            DistanceFromVWAP REAL,        -- Percentage value (e.g. 0.75 for 0.75%)
            IsActive INTEGER DEFAULT 1,   -- 1 = Active, 0 = Inactive
            IsRetested INTEGER DEFAULT 0, -- 1 = Retested, 0 = Not Retested
            Priority INTEGER,             -- Priority ranking or weight
            LastModifiedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );         
    """,
    "RetestGap": """
        CREATE TABLE IF NOT EXISTS RetestGap (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Symbol TEXT NOT NULL,
            OpenTime DATETIME NOT NULL,
            FairValueGap INTEGER NOT NULL,
            TimeFrame TEXT NOT NULL,
            Direction TEXT CHECK(Direction IN ('Bullish', 'Bearish')),
            Type TEXT,
            Open REAL,
            High REAL,
            Low REAL,
            Close REAL,
            Volume REAL,
            IsActive INTEGER DEFAULT 1,
            IsTraded INTEGER DEFAULT 0,
            LastModifiedDate TIMESTAMP,
            FOREIGN KEY (FairValueGap) REFERENCES FairValueGap(Id)
        );
    """,
    "Trades":"""
      CREATE TABLE IF NOT EXISTS Trades (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            EntryTime TEXT NOT NULL,
            RetestGap INTEGER,
            EntryCandle TEXT,
            Open REAL,
            High REAL,
            Low REAL,
            Close REAL,
            Volume REAL,
            Direction TEXT,
            CandleType TEXT,
            Stratagy TEXT,
            Lot INTEGER,
            RemainingLot INTEGER,
            IntialStopLoss REAL,
            IntialTarget REAL,
            ModifiedStopLoss REAL,
            ModifiedTarget REAL,
            LastModifiedDate TIMESTAMP DEFAULT (CURRENT_TIMESTAMP),
            IsActive INTEGER DEFAULT 1,
            Symbol TEXT,
            FOREIGN KEY (RetestGap) REFERENCES RetestGap(Id)
        );
    """,
    "TradeStatus":"""
      CREATE TABLE IF NOT EXISTS TradeStatus (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Symbol TEXT NOT NULL,
            EntryTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            Trade INTEGER NOT NULL,
            EntryPrice REAL NOT NULL,
            ExitPrice REAL,
            Pnl REAL,
            Duration INTEGER,
            Status TEXT,
            Quantity INTEGER,
            IsOpen INTEGER DEFAULT 1,
            LastModifiedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            FOREIGN KEY (Trade) REFERENCES Trades(Id)
        );
    """
}
