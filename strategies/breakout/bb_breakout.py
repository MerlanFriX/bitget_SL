import datetime
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_DIR))

sys.path.append("./bitget_SL")

import asyncio
from utilities.bitget_perp import PerpBitget
from utilities.discord_logger import DiscordLogger
from utilities.custom_indicators import Breakout
from secret import ACCOUNTS
import ta
import math
import copy
import json
import numpy as np


MARGIN_MODE = "isolated" # isolated or cross
LEVERAGE = 1.5
ACCOUNT_NAME = "bitget_main"
SIDE = ["long","short"]
DISCORD_WEBHOOK = ""
TIMEFRAME = "1h"
PARAMS = {
    "BTC/USDT":{
        "size": 0.1,
        "bb_window": 20,
        "bb_std": 2,
        "volume_window": 20,
        "volume_multiplier": 2,
    },
    "ETH/USDT":{
        "size": 0.1,
        "bb_window": 20,
        "bb_std": 2,
        "volume_window": 20,
        "volume_multiplier": 2,
    },
    "MAGIC/USDT":{
        "size": 0.1,
        "bb_window": 20,
        "bb_std": 2,
        "volume_window": 20,
        "volume_multiplier": 2,
    },
    "DOGE/USDT":{
        "size": 0.1,
        "bb_window": 20,
        "bb_std": 2,
        "volume_window": 20,
        "volume_multiplier": 2,
    },
    "ADA/USDT":{
        "size": 0.1,
        "bb_window": 20,
        "bb_std": 2,
        "volume_window": 20,
        "volume_multiplier": 2,
    },
    "XRP/USDT":{
        "size": 0.1,
        "bb_window": 20,
        "bb_std": 2,
        "volume_window": 20,
        "volume_multiplier": 2,
    },
    "BNB/USDT":{
        "size": 0.1,
        "bb_window": 20,
        "bb_std": 2,
        "volume_window": 20,
        "volume_multiplier": 2,
    },
     "SOL/USDT":{
        "size": 0.1,
        "bb_window": 20,
        "bb_std": 2,
        "volume_window": 20,
        "volume_multiplier": 2,
    },
      "PEPE/USDT":{
        "size": 0.1,
        "bb_window": 20,
        "bb_std": 2,
        "volume_window": 20,
        "volume_multiplier": 2,
    },
       "SHIB/USDT":{
        "size": 0.1,
        "bb_window": 20,
        "bb_std": 2,
        "volume_window": 20,
        "volume_multiplier": 2,
    },
}

RELATIVE_PATH = "./strategies/breakout"

POSITIONS_FILE = Path(RELATIVE_PATH) / f"positions_{ACCOUNT_NAME}.json"

def load_positions():
    #ensure .json exist
    POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not POSITIONS_FILE.exists():
        save_positions({})
        return {}
    with open(POSITIONS_FILE, "r") as f:
        return json.load(f)

def save_positions(data):
    # ensure folder exists
    POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(POSITIONS_FILE, "w") as f:
        json.dump(data, f, indent=2)


if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def main():
    account = ACCOUNTS[ACCOUNT_NAME]
    margin_mode = MARGIN_MODE
    leverage = LEVERAGE
    exchange_leverage = math.ceil(leverage)
    params = PARAMS
    dl = DiscordLogger(DISCORD_WEBHOOK)
    exchange = PerpBitget(
        public_api=account["public_api"],
        secret_api=account["secret_api"],
        password=account["password"],
        testnet=True,
    )
    await exchange.load_markets()
    
    print(f"--- Execution started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    # Read json position file, if not exist, create it
    key_positions = load_positions()
        
    try:
        
        pair_list = []
        key_params = {}

        for pair in PARAMS:
            key_params[pair] = PARAMS[pair]
            key_params[pair]["pair"] = pair
            key_params[pair]["tf"] = TIMEFRAME
            pair_list.append(pair)

        # Validate and filter out unavailable pairs
        key_params_copy = copy.deepcopy(key_params)
        for pair in list(key_params_copy.keys()):  # safer to loop over a copy of keys
            info = exchange.get_pair_info(pair)
            if info is None:
                await dl.send_now(
                    f"⚠️ Pair {pair} not found on exchange, removing from list.",
                    level="WARNING"
                )
                del key_params[pair]
                pair_list.remove(pair)
                
        print(f"Getting data and indicators on {len(pair_list)} pairs...")
        tasks = []
        keys = []
        tf_pair_loaded = []
        for key_param in key_params.keys():
            key_param_object = key_params[key_param]
            # Check if param have a size
            if "size" not in key_param_object.keys():
                key_param_object["size"] = 1/len(key_params)
            if f"{key_param_object['pair']}-{key_param_object['tf']}" not in tf_pair_loaded:
                tf_pair_loaded.append(f"{key_param_object['pair']}-{key_param_object['tf']}")
                keys.append(f"{key_param_object['pair']}-{key_param_object['tf']}")

                tasks.append(exchange.get_last_ohlcv(key_param_object["pair"], key_param_object["tf"], 600))

        dfs = await asyncio.gather(*tasks)
        df_data = dict(zip(keys, dfs))
        df_list = {}
        
        #Breakout indicator from custom_indicators.py
        
        for key_param in key_params.keys():
            key_param_object = key_params[key_param]
            df = df_data[f"{key_param_object['pair']}-{key_param_object['tf']}"]

            breakout_obj = Breakout(
                close=df["close"],
                volume=df["volume"],
                bb_window=key_param_object.get("bb_window", 20),
                bb_std=key_param_object.get("bb_std", 2),
                volume_window=key_param_object.get("volume_window", 20),
                volume_multiplier=key_param_object.get("volume_multiplier", 2),
                atr_window=key_param_object.get("atr_window", 20),
            )
            
            df["bb_high"] = breakout_obj.get_bb_high()
            df["bb_low"] = breakout_obj.get_bb_low()
            df["bb_mid"] = breakout_obj.get_bb_mid()
            df["vol_spike"] = breakout_obj.get_vol_spike()
            df["breakout_up"] = breakout_obj.get_breakout_up()
            df["breakout_down"] = breakout_obj.get_breakout_down()
            df["atr"] = breakout_obj.get_atr()

            df.dropna(inplace=True)
            df_list[key_param] = df
        
        # --- Debug print to see last row and breakout signals ---
        for key, df in df_list.items():
            print(f"Pair {key}, last close: {df.iloc[-1]['close']}, breakout_up: {df.iloc[-1].get('breakout_up')}, breakout_down: {df.iloc[-1].get('breakout_down')}")
        
        usdt_balance = await exchange.get_balance()
        usdt_balance = usdt_balance.total
        dl.log(f"Balance: {round(usdt_balance, 2)} USDT")

        # Keep track of which pairs are already configured
        configured_pairs = []

        positions = await exchange.get_open_positions(pair_list)
        long_exposition = sum([p.usd_size for p in positions if p.side == "long"])
        short_exposition = sum([p.usd_size for p in positions if p.side == "short"])
        unrealized_pnl = sum([p.unrealizedPnl for p in positions])
        dl.log(f"Unrealized PNL: {round(unrealized_pnl, 2)}$ | Long Exposition: {round(long_exposition, 2)}$ | Short Exposition: {round(short_exposition, 2)}$")
        dl.log(f"Current positions:")
        for position in positions:
            dl.log(f"{(position.side).upper()} {position.size} {position.pair} ~{position.usd_size}$ (+ {position.unrealizedPnl}$)")

        try:
            print(f"Setting {margin_mode} x{exchange_leverage} on {len(pair_list)} pairs...")

            for pair in pair_list:
                # Skip pairs that already have an open position
                if pair in [position.pair for position in positions]:
                    continue
                # Skip pairs that were already configured
                if pair in configured_pairs:
                    print(f"⚠️ Skipping {pair}, margin/leverage already set")
                    continue
                
                
                print(f"⚙️ Setting margin mode and leverage for {pair}...")

                try:
                    params = {
                        "marginCoin": "USDT",           # required for Bitget
                        "productType": "USDT-FUTURES",  # defines USDT-margined swap
                    }

                    await exchange._session.set_margin_mode(
                        margin_mode,
                        pair,
                        params=params
                    )

                    await asyncio.sleep(0.3)

                    await exchange._session.set_leverage(
                        exchange_leverage,
                        pair,
                        params=params
                    )

                    print(f"✅ Done {pair}")
                    await asyncio.sleep(0.3)

                except Exception as e:
                    print(f"❌ Error setting leverage/margin for {pair}: {e}")
        except Exception as e:
                print("❌ Unexpected error while setting leverage/margin:", e)

        # --- Close positions ---
        key_positions_copy = copy.deepcopy(key_positions)
        print("key_positions:", key_positions)
        print("positions returned:", positions)
        print("df_list keys:", df_list.keys())
        if not key_positions_copy:
            print("No saved positions to process, skipping close logic.")

        for key_position in key_positions_copy:
            position_object = key_positions_copy[key_position]
            param_object = key_params[key_position]
            df = df_list.get(key_position)
            if df is None or len(df) < 2:
                print(f"Not enough data for {param_object['pair']}, skipping...")
                continue
            exchange_positions = [p for p in positions if (p.pair == param_object["pair"] and p.side == position_object["side"])]
            if len(exchange_positions) == 0:
                print(f"No position found for {param_object['pair']}, skipping...")
                continue
            exchange_position_size = sum([p.size for p in exchange_positions])
            if len(df) < 2:
                dl.log(f"Not enough data for {param_object['pair']}, skipping...")
                continue
            
            row = df.iloc[-1]  # you can also consider using the last row df.iloc[-1]
            current_price = row["close"]
            atr_value = row.get("atr")
            if not np.isfinite(atr_value):
                dl.log(f"Invalid ATR for {param_object['pair']}, skipping...")
                continue
            entry_price = position_object.get("entry_price")

            if not entry_price or atr_value is None:
                print(f"Missing entry price or ATR for {param_object['pair']}, skipping stop loss check.")
                continue

            # --- ATR STOP LOSS CHECK ---
            stop_loss_hit = False
            if position_object["side"] == "long":
                stop_loss_price = entry_price - atr_value
                stop_loss_hit = current_price <= stop_loss_price
            elif position_object["side"] == "short":
                stop_loss_price = entry_price + atr_value
                stop_loss_hit = current_price >= stop_loss_price

            # --- BREAKOUT LOGIC (if stop loss not triggered) ---
            if stop_loss_hit:
                close_size = min(position_object["size"], exchange_position_size)
                try:
                    order = await exchange.place_order(
                        pair=param_object["pair"],
                        side="sell" if position_object["side"] == "long" else "buy",
                        price=None,
                        size=close_size,
                        type="market",
                        reduce=True,
                        margin_mode=margin_mode,
                        leverage=math.ceil(leverage),
                        error=True,
                    )
                    if order is not None:
                        del key_positions[key_position]
                        save_positions(key_positions)
                        dl.log(f"🛑 {key_position} STOP LOSS hit: Closed {order.size} {param_object['pair']} {position_object['side']} at {current_price} (ATR SL)")
                    continue  # skip breakout check if SL was hit
                except Exception as e:
                    await dl.send_now(f"❌ {key_position} Error closing {param_object['pair']} due to ATR stop loss: {e}", level="ERROR")
                    continue

            if position_object["side"] == "long":
                # Close condition for long: breakout_down signal True
                if row.get("breakout_down", False):
                    close_size = min(position_object["size"], exchange_position_size)
                    try:
                        order = await exchange.place_order(
                            pair=param_object["pair"],
                            side="sell",
                            price=None,
                            size=close_size,
                            type="market",
                            reduce=True,
                            margin_mode=margin_mode,
                            leverage=math.ceil(leverage),
                            error=True,
                        )
                        if order is not None:
                            del key_positions[key_position]
                            save_positions(key_positions)
                            dl.log(f"{key_position} Closed {order.size} {param_object['pair']} long due to breakout_down")
                    except Exception as e:
                        await dl.send_now(f"{key_position} Error closing {param_object['pair']} long: {e}", level="ERROR")
                        continue

            elif position_object["side"] == "short":
                # Close condition for short: breakout_up signal True
                if row.get("breakout_up", False):
                    close_size = min(position_object["size"], exchange_position_size)
                    try:
                        order = await exchange.place_order(
                            pair=param_object["pair"],
                            side="buy",
                            price=None,
                            size=close_size,
                            type="market",
                            reduce=True,
                            margin_mode=margin_mode,
                            leverage=math.ceil(leverage),
                            error=True,
                        )
                        if order is not None:
                            del key_positions[key_position]
                            save_positions(key_positions)
                            dl.log(f"{key_position} Closed {order.size} {param_object['pair']} short due to breakout_up")
                    except Exception as e:
                        await dl.send_now(f"{key_position} Error closing {param_object['pair']} short: {e}", level="ERROR")
                        continue
            
        # --- Open new positions ---
        for key, df in df_list.items():
            if len(df) < 2:
                continue

            row = df.iloc[-1]
            param = key_params[key]

            # Skip if already in a position
            if key in key_positions:
                continue

            entry_size = param["size"]

            # LONG ENTRY
            if row.get("breakout_up", False) and row.get("vol_spike", False):
                try:
                    order = await exchange.place_order(
                        pair=param["pair"],
                        side="buy",
                        price=None,
                        size=entry_size,
                        type="market",
                        margin_mode=margin_mode,
                        leverage=math.ceil(leverage),
                        error=True,
                    )
                    if order is not None:
                        key_positions[key] = {
                            "pair": param["pair"],
                            "side": "long",
                            "size": order.size,
                            "entry_price": row["close"],
                        }
                        save_positions(key_positions)
                        dl.log(f"✅ OPENED LONG {order.size} {param['pair']} at {row['close']}")
                except Exception as e:
                    await dl.send_now(f"❌ Error opening long on {param['pair']}: {e}", level="ERROR")

            # SHORT ENTRY
            elif row.get("breakout_down", False) and row.get("vol_spike", False):
                try:
                    order = await exchange.place_order(
                        pair=param["pair"],
                        side="sell",
                        price=None,
                        size=entry_size,
                        type="market",
                        margin_mode=margin_mode,
                        leverage=math.ceil(leverage),
                        error=True,
                    )
                    if order is not None:
                        key_positions[key] = {
                            "pair": param["pair"],
                            "side": "short",
                            "size": order.size,
                            "entry_price": row["close"],
                        }
                        save_positions(key_positions)
                        dl.log(f"✅ OPENED SHORT {order.size} {param['pair']} at {row['close']}")
                except Exception as e:
                    await dl.send_now(f"❌ Error opening short on {param['pair']}: {e}", level="ERROR")
    except Exception as main_e:
        # top-level exception handler inside main
        print(f"❌ Error in main: {main_e}")
        try:
            await dl.send_now(f"❌ Bot crashed: {main_e}", level="ERROR")
        except Exception:
            pass
    finally:
        # ensure positions are saved and exchange cleaned up on exit
        try:
            save_positions(key_positions)
        except Exception as e:
            print(f"⚠️ Failed to save positions on exit: {e}")
        # if your PerpBitget has an async close method (e.g., to close aiohttp session), call it
        try:
            if 'exchange' in locals() and hasattr(exchange, "close"):
                maybe_close = exchange.close
                if asyncio.iscoroutinefunction(maybe_close):
                    await exchange.close()
                else:
                    exchange.close()
        except Exception as e:
            print(f"⚠️ Failed to close exchange session cleanly: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n--- Manual stop (Ctrl+C) ---")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")