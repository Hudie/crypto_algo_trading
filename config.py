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
MINIMUM_TICK_SIZE = 0.5
PERPETUAL = 'BTC-PERPETUAL'
SEASON_FUTURE = 'BTC-26JUN20'

SIZE_PER_TRADE = 900
TX_ENTRY_GAP = [4.5, 5.5, 6.5, 7.5, 8.5, 9.5]		# premium rate
TX_EXIT_GAP = 0.8					# premium rate
TX_ENTRY_PRICE_GAP = 0.3			# percentage of current price
POSITION_SIZE_THRESHOLD = [300000 * i for i in [1, 2, 3, 4, 5, 6]]
# no open when first threshold is met, and close position when second threshold is met
MARGIN_THRESHOLD = [0.9, 0.92]

# parameters for next quarterly future
N_QUARTERLY_FUTURE = 'BTC-25SEP20'

N_SIZE_PER_TRADE = 960
N_TX_ENTRY_GAP = [4.5, 5.5, 6.5, 7.5, 8.5, 9.5]
N_TX_EXIT_GAP = 2
N_TX_ENTRY_PRICE_GAP = 0.3
N_POSITION_SIZE_THRESHOLD = [200000 * i for i in [1, 2, 3, 4, 5, 6]]
N_MARGIN_THRESHOLD = [0.9, 0.92]