# -*- coding: utf-8 -*-

DERIBIT_ACCOUNT_ID = 'mogu1988'
DERIBIT_CLIENT_ID = 'PmyJIl5T'
DERIBIT_CLIENT_SECRET = '7WBI4N_YT8YB5nAFq1VjPFedLMxGfrxxCbreMFOYLv0'

# N_ means next quarterly future
N_DERIBIT_ACCOUNT_ID = 'maxlu'
N_DERIBIT_CLIENT_ID = 'RZ4pks_D'
N_DERIBIT_CLIENT_SECRET = 'i62Zz0piJN3YSFvjTITkLegso-NpZVj0iL1anhxh0Vc'

# parameters for arbitrage between perpetual and future
SYMBOL = 'BTC'
PERPETUAL = 'BTC-PERPETUAL'
SEASON_FUTURE = 'BTC-26JUN20'

SIZE_PER_TRADE = 300
TX_ENTRY_GAP = [42, 52, 62, 72, 82, 102, 122, 142]
TX_EXIT_GAP = 8
POSITION_SIZE_THRESHOLD = [250000 * i for i in [1, 1.5, 2, 2.5, 3, 4, 5, 6]]

# parameters for next quarterly future
N_QUARTERLY_FUTURE = 'BTC-25SEP20'

N_SIZE_PER_TRADE = 360
N_TX_ENTRY_GAP = [82, 102, 122, 152, 192, 242, 302, 372]
N_TX_EXIT_GAP = 38
N_POSITION_SIZE_THRESHOLD = [250000 * i for i in [1, 2, 3, 4, 5, 6, 7, 8]]
