def detect_fvg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect Fair Value Gaps (FVG) in OHLC data.

    Adds:
      - FVG_Start: numeric, start of the gap
      - FVG_End: numeric, end of the gap
      - FVG_Type: object, 'Positive' or 'Negative'

    Ignores gaps smaller than 0.05% of Close price.
    """
    df = df.copy()

    # Shift for n-2 candles safely
    df['High_prev2'] = df['High'].shift(2)
    df['Low_prev2'] = df['Low'].shift(2)

    # Vectorized conditions
    bullish_mask = df['Low'] > df['High_prev2']
    bearish_mask = df['High'] < df['Low_prev2']

    # Assign numeric columns (vectorized)
    df['FVG_Start'] = np.where(bullish_mask, df['High_prev2'],
                        np.where(bearish_mask, df['Low_prev2'], np.nan))

    df['FVG_End'] = np.where(bullish_mask, df['Low'],
                      np.where(bearish_mask, df['High'], np.nan))

    # Ignore small gaps (<0.05% of Close)
    gap_size = (df['FVG_Start'] - df['FVG_End']).abs()
    small_gap_mask = gap_size < (0.0005 * df['Close'])  # 0.05%
    df.loc[small_gap_mask, ['FVG_Start', 'FVG_End']] = np.nan
    # Also clear type for these small gaps
    df['FVG_Type'] = np.empty(len(df), dtype='object')

    # Assign values again after filtering small gaps
    df.loc[bullish_mask & ~small_gap_mask, 'FVG_Type'] = 'Positive'
    df.loc[bearish_mask & ~small_gap_mask, 'FVG_Type'] = 'Negative'
    df.loc[~((bullish_mask & ~small_gap_mask) | (bearish_mask & ~small_gap_mask)), 'FVG_Type'] = None

    # Clean up helper columns
    df.drop(columns=['High_prev2', 'Low_prev2'], inplace=True)

    return df


# --- VWAP calculation ---
def add_vwap(df):
    typical_price = (df["High"] + df["Low"] + df["Close"]) / 3
    cumulative_vol = df["Volume"].cumsum()
    cumulative_vp = (typical_price * df["Volume"]).cumsum()
    df["VWAP"] = cumulative_vp / cumulative_vol
    return df


def forward_fill_fvg(df_1min):
    """
    Forward-fill Active_FVG_Start, Active_FVG_End, Active_FVG_Type in 1-min dataframe
    based on OpenTime.
    
    Parameters:
    df_1min : pd.DataFrame
        1-minute OHLCV data with columns: 'OpenTime', 'Active_FVG_Start', 'Active_FVG_End', 'Active_FVG_Type'
        
    Returns:
    pd.DataFrame
        DataFrame with forward-filled Active_FVG columns
    """
    # Ensure OpenTime is datetime and sorted
    df_1min['OpenTime'] = pd.to_datetime(df_1min['OpenTime'])
    df_1min = df_1min.sort_values('OpenTime').reset_index(drop=True)
    
    # Forward fill the Active_FVG columns
    df_1min[['Active_FVG_Start', 'Active_FVG_End', 'Active_FVG_Type']] = df_1min[['Active_FVG_Start', 'Active_FVG_End', 'Active_FVG_Type']].ffill()
    
    return df_1min


def add_active_fvg(df_1min, df_5min):
    """
    Adds Active_FVG_Start, Active_FVG_End, Active_FVG_Type columns to 1-min dataframe
    using the most recent 5-min candle FVG values.
    
    Parameters:
    df_1min : pd.DataFrame
        1-minute OHLCV data with column 'OpenTime'
    df_5min : pd.DataFrame
        5-minute OHLCV data with columns 'OpenTime', 'FVG_Start', 'FVG_End', 'FVG_Type'
        
    Returns:
    pd.DataFrame
        1-minute dataframe with added Active_FVG columns
    """
    # Ensure datetime format
    df_1min['OpenTime'] = pd.to_datetime(df_1min['OpenTime'])
    df_5min['OpenTime'] = pd.to_datetime(df_5min['OpenTime'])
    
    # Sort dataframes by time
    df_1min = df_1min.sort_values('OpenTime')
    df_5min = df_5min.sort_values('OpenTime')
    
    # Merge using asof to forward-fill the FVG values
    df_merged = pd.merge_asof(
        df_1min, 
        df_5min[['OpenTime', 'FVG_Start', 'FVG_End', 'FVG_Type']], 
        on='OpenTime', 
        direction='backward'
    )
    
    # Rename columns to Active_FVG_*
    df_merged.rename(columns={
        'FVG_Start': 'Active_FVG_Start',
        'FVG_End': 'Active_FVG_End',
        'FVG_Type': 'Active_FVG_Type'
    }, inplace=True)
    
    return forward_fill_fvg(df_merged)


def add_fvg_triggered_forward(df):
    # Initialize
    df['FVG_Triggered'] = ''

    # Create a group identifier for each active FVG
    df['FVG_Group'] = df['Active_FVG_Start'].astype(str) + '_' + df['Active_FVG_End'].astype(str)

    # Process each FVG group
    for grp, group_df in df.groupby('FVG_Group', sort=False):
        if grp == 'nan_nan':  # skip rows without FVG
            continue
        
        # Find first row where FVG is triggered
        if group_df['Active_FVG_Type'].iloc[0] == 'Negative':
            trigger_idx = group_df.index[group_df['High'] > group_df['Active_FVG_End']].min()
        else:  # Positive FVG
            trigger_idx = group_df.index[group_df['Low'] < group_df['Active_FVG_End']].min()
        
        if pd.notna(trigger_idx):
            # Forward fill 'Y' from trigger row to the rest of the group
            df.loc[group_df.index[group_df.index >= trigger_idx], 'FVG_Triggered'] = 'Y'
    
    # Drop helper column
    df.drop(columns='FVG_Group', inplace=True)
    
    return df



def add_fvg_trigger_levels(df):
    # Initialize new columns
    df['FVG_Positive_Trigger_Low'] = np.nan
    df['FVG_Negative_Trigger_High'] = np.nan

    # Create a group identifier for each active FVG
    df['FVG_Group'] = df['Active_FVG_Start'].astype(str) + '_' + df['Active_FVG_End'].astype(str)

    # Process each FVG group
    for grp, group_df in df.groupby('FVG_Group', sort=False):
        if grp == 'nan_nan':  # skip rows without FVG
            continue
        
        fvg_type = group_df['Active_FVG_Type'].iloc[0]

        # Find first row where FVG is triggered
        if fvg_type == 'Negative':
            trigger_idx = group_df.index[group_df['High'] > group_df['Active_FVG_End']].min()
            if pd.notna(trigger_idx):
                # Fill Negative FVG High for the entire group
                df.loc[group_df.index, 'FVG_Negative_Trigger_High'] = group_df.loc[trigger_idx, 'Low']
        elif fvg_type == 'Positive':
            trigger_idx = group_df.index[group_df['Low'] < group_df['Active_FVG_End']].min()
            if pd.notna(trigger_idx):
                # Fill Positive FVG Low for the entire group
                df.loc[group_df.index, 'FVG_Positive_Trigger_Low'] = group_df.loc[trigger_idx, 'High']

    # Drop helper column
    df.drop(columns='FVG_Group', inplace=True)
    
    return df



def add_fvg_broken_first_only(df):
    # Initialize column
    df['FVG_Broken'] = ''

    # Create FVG group identifier
    df['FVG_Group'] = df['Active_FVG_Start'].astype(str) + '_' + df['Active_FVG_End'].astype(str)

    # Iterate per FVG group
    for grp, group_df in df.groupby('FVG_Group', sort=False):
        if grp == 'nan_nan':  # skip rows without FVG
            continue
        
        fvg_type = group_df['Active_FVG_Type'].iloc[0]

        # Filter only after FVG is triggered
        group_df = group_df[group_df['FVG_Triggered'] == 'Y']
        if group_df.empty:
            continue

        # --- NEGATIVE FVG ---
        if fvg_type == 'Negative' and group_df['FVG_Negative_Trigger_High'].notna().any():
            trigger_high = group_df['FVG_Negative_Trigger_High'].iloc[0]
            broken_idx = group_df.index[group_df['Close'] < trigger_high].min()
            if pd.notna(broken_idx):
                df.loc[broken_idx, 'FVG_Broken'] = 'Y'  # ✅ only first break

        # --- POSITIVE FVG ---
        elif fvg_type == 'Positive' and group_df['FVG_Positive_Trigger_Low'].notna().any():
            trigger_low = group_df['FVG_Positive_Trigger_Low'].iloc[0]
            broken_idx = group_df.index[group_df['Close'] > trigger_low].min()
            if pd.notna(broken_idx):
                df.loc[broken_idx, 'FVG_Broken'] = 'Y'  # ✅ only first break

    # Drop helper column
    df.drop(columns='FVG_Group', inplace=True)

    return df



def add_fvg_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Initialize new columns with NaN
    df['Difference'] = np.nan
    df['SL'] = np.nan
    df['Target'] = np.nan
    
    # Apply only when FVG_Broken == 'Y'
    mask = df['FVG_Broken'] == 'Y'
    
    # --- Difference ---
    df.loc[mask & (df['Active_FVG_Type'] == 'Negative'), 'Difference'] = \
        (df['Close'] - df['Low']) / df['Close'] * 100

    df.loc[mask & (df['Active_FVG_Type'] == 'Positive'), 'Difference'] = \
        (df['High'] - df['Close']) / df['Close'] * 100

    # --- SL (Stop Loss) ---
    df.loc[mask & (df['Active_FVG_Type'] == 'Negative'), 'SL'] = \
        df['Active_FVG_Start'] + df['Active_FVG_Start'] * 0.0001

    df.loc[mask & (df['Active_FVG_Type'] == 'Positive'), 'SL'] = \
        df['Active_FVG_Start'] - df['Active_FVG_Start'] * 0.0001

    # --- Target ---
    # For Negative FVG
    neg_mask = mask & (df['Active_FVG_Type'] == 'Negative')
    df.loc[neg_mask, 'Target'] = df.loc[neg_mask, 'Close'] - (df.loc[neg_mask, 'SL'] - df.loc[neg_mask, 'Close']) * 3

    # For Positive FVG
    pos_mask = mask & (df['Active_FVG_Type'] == 'Positive')
    df.loc[pos_mask, 'Target'] = df.loc[pos_mask, 'Close'] + (df.loc[pos_mask, 'Close'] - df.loc[pos_mask, 'SL']) * 3
    
    return df



def add_result_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['Result'] = pd.Series(dtype='object')  # <-- Fix dtype issue

    # Ensure sorting by time
    df = df.sort_values('OpenTime').reset_index(drop=True)

    for i in range(len(df)):
        if df.loc[i, 'FVG_Broken'] == 'Y':
            fvg_type = df.loc[i, 'Active_FVG_Type']
            target = df.loc[i, 'Target']
            sl = df.loc[i, 'SL']
            start_time = df.loc[i, 'OpenTime']

            # Subset of future candles
            future = df[df['OpenTime'] > start_time]

            result = None
            for _, row in future.iterrows():
                if fvg_type == 'Positive':
                    if row['High'] >= target:
                        result = 'TG'
                        break
                    elif row['Low'] <= sl:
                        result = 'SL'
                        break

                elif fvg_type == 'Negative':
                    if row['Low'] <= target:
                        result = 'TG'
                        break
                    elif row['High'] >= sl:
                        result = 'SL'
                        break

            df.at[i, 'Result'] = result

    return df