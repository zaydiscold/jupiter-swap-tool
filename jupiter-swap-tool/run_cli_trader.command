#!/bin/bash
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR" || exit 1

if ! command -v node >/dev/null 2>&1; then
  echo "Node.js is not installed or not in PATH."
  read -p "Press Enter to close this window..." _
  exit 1
fi

DEFAULT_RPC="${RPC_URL:-https://mainnet.helius-rpc.com/?api-key=98a9fb2e-26c6-4420-b0bf-a38ece2eb907}"
printf '\033[96m'
cat <<'BANNER'
     ____.                   _____    ________    _____                      
    |    |__ ________       /  _  \  /  _____/  _/ ____\____ _______  _____  
    |    |  |  \____ \     /  /_\  \/   \  ___  \   __\\__  \\_  __ \/     \ 
/\__|    |  |  /  |_> >   /    |    \    \_\  \  |  |   / __ \|  | \/  Y Y  \
\________|____/|   __/ /\ \____|__  /\______  /  |__|  (____  /__|  |__|_|  /
               |__|    \/         \/        \/              \/            \/
BANNER

printf 'Jupiter Swap Tool v1.2.1 — made by zayd / cold\033[0m\n'

echo "Jupiter Swap Tool CLI launcher"
read -r -p "RPC URL [${DEFAULT_RPC}]: " USER_RPC
if [[ -n "$USER_RPC" ]]; then
  export RPC_URL="$USER_RPC"
else
  export RPC_URL="$DEFAULT_RPC"
fi
export NODE_NO_WARNINGS=1
read -r -p "Default swap amount (all/random) [all]: " SWAP_MODE_INPUT
SWAP_MODE_INPUT=$(printf '%s' "$SWAP_MODE_INPUT" | tr '[:upper:]' '[:lower:]')
if [[ "$SWAP_MODE_INPUT" != "random" ]]; then
  SWAP_MODE_INPUT="all"
fi
export SWAP_AMOUNT_MODE="$SWAP_MODE_INPUT"
export JUPITER_SWAP_TOOL_NO_BANNER=1
printf '▶ RPC: %s\n' "$RPC_URL"
printf '▶ Swap mode: %s\n\n' "$SWAP_AMOUNT_MODE"

SOL_MINT="So11111111111111111111111111111111111111112"
USDC_MAINNET="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
POPCAT_MINT="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
CREW_WALLET="crew_1.json"

KEYPAIRS_DIR="keypairs"
KEYPAIRS_EXISTED_BEFORE=0
if [[ -d "$KEYPAIRS_DIR" ]]; then
  KEYPAIRS_EXISTED_BEFORE=1
fi

RPC_FILE_CONFIG="${RPC_LIST_FILE:-rpc_endpoints.txt}"
RPC_FILE_EXISTED_BEFORE=0
if [[ -f "$RPC_FILE_CONFIG" ]]; then
  RPC_FILE_EXISTED_BEFORE=1
fi

WALLET_COUNT="..."
GUARD_STATUS_READY=0
GUARD_MODE=""
GUARD_ACTIVE=""
GUARD_TOTAL=""
GUARD_DISABLED=""
GUARD_DISABLED_SAMPLE=""
GUARD_LAST_COMPUTED=""
GUARD_REFRESH_NEEDED=0
GUARD_REFRESHING=0
GUARD_REFRESH_PID=""
GUARD_REFRESH_FILE=""

reset_launcher_state() {
  WALLET_COUNT="..."
  GUARD_STATUS_READY=0
  GUARD_MODE=""
  GUARD_ACTIVE=""
  GUARD_TOTAL=""
  GUARD_DISABLED=""
  GUARD_DISABLED_SAMPLE=""
  GUARD_LAST_COMPUTED=""
  if [[ -z "$GUARD_REFRESH_PID" ]]; then
    GUARD_REFRESHING=0
  fi
  GUARD_REFRESH_NEEDED=0
}

cleanup_guard_refresh_state() {
  if [[ -n "$GUARD_REFRESH_PID" ]]; then
    wait "$GUARD_REFRESH_PID" 2>/dev/null
  fi
  if [[ -n "$GUARD_REFRESH_FILE" && -f "$GUARD_REFRESH_FILE" ]]; then
    rm -f "$GUARD_REFRESH_FILE"
  fi
}

trap cleanup_guard_refresh_state EXIT

parse_launcher_state() {
  local data="$1"
  local hadData=0
  while IFS='=' read -r key value; do
    hadData=1
    case "$key" in
      walletCount)
        WALLET_COUNT="$value"
        ;;
      guardMode)
        GUARD_MODE="$value"
        ;;
      guardActive)
        GUARD_ACTIVE="$value"
        ;;
      guardTotal)
        GUARD_TOTAL="$value"
        ;;
      guardDisabled)
        GUARD_DISABLED="$value"
        ;;
      guardDisabledSample)
        GUARD_DISABLED_SAMPLE="$value"
        ;;
      guardLastComputedAt)
        if [[ -z "$value" || "$value" == "null" ]]; then
          GUARD_LAST_COMPUTED=""
        else
          GUARD_LAST_COMPUTED="$value"
        fi
        ;;
      needsRefresh)
        if [[ -z "$GUARD_REFRESH_PID" ]]; then
          if [[ "$value" == "true" ]]; then
            GUARD_REFRESH_NEEDED=1
          else
            GUARD_REFRESH_NEEDED=0
          fi
        fi
        ;;
    esac
  done <<<"$data"
  if [[ $hadData -eq 1 ]]; then
    GUARD_STATUS_READY=1
  fi
}

load_launcher_state() {
  local output
  output=$(
    JUPITER_SWAP_TOOL_SKIP_INIT=1 \
    JUPITER_SWAP_TOOL_NO_BANNER=1 \
    node cli_trader.js launcher-bootstrap 2>/dev/null
  )
  local status=$?
  if [[ $status -ne 0 || -z "$output" ]]; then
    GUARD_STATUS_READY=0
    return 1
  fi
  parse_launcher_state "$output"
  return 0
}

start_guard_refresh() {
  if [[ "$GUARD_MODE" == "force-reset" ]]; then
    GUARD_REFRESH_NEEDED=0
    return
  fi
  if [[ -n "$GUARD_REFRESH_PID" ]]; then
    return
  fi
  GUARD_REFRESH_FILE=$(mktemp -t jup_guard_refresh.XXXXXX)
  (
    JUPITER_SWAP_TOOL_SKIP_INIT=1 \
    JUPITER_SWAP_TOOL_NO_BANNER=1 \
    node cli_trader.js launcher-bootstrap --refresh >"$GUARD_REFRESH_FILE" 2>&1
  ) &
  GUARD_REFRESH_PID=$!
  GUARD_REFRESHING=1
  echo "Refreshing wallet guard summary in the background..."
}

reap_guard_refresh() {
  if [[ -z "$GUARD_REFRESH_PID" ]]; then
    return
  fi
  if kill -0 "$GUARD_REFRESH_PID" 2>/dev/null; then
    return
  fi
  wait "$GUARD_REFRESH_PID"
  local status=$?
  if [[ -f "$GUARD_REFRESH_FILE" ]]; then
    local data
    data=$(cat "$GUARD_REFRESH_FILE")
    rm -f "$GUARD_REFRESH_FILE"
    GUARD_REFRESH_FILE=""
    if [[ $status -eq 0 ]]; then
      parse_launcher_state "$data"
      GUARD_REFRESH_NEEDED=0
      GUARD_STATUS_READY=1
      GUARD_REFRESHING=0
      GUARD_REFRESH_PID=""
      echo
      echo "Wallet guard summary refreshed."
      print_hotkeys
      return
    else
      echo
      echo "Warning: wallet guard refresh failed:"
      echo "$data"
    fi
  fi
  GUARD_REFRESH_PID=""
  GUARD_REFRESHING=0
}

describe_last_update() {
  if [[ -z "$GUARD_LAST_COMPUTED" ]]; then
    echo ""
    return
  fi
  if ! [[ "$GUARD_LAST_COMPUTED" =~ ^[0-9]+$ ]]; then
    echo ""
    return
  fi
  local now
  now=$(date +%s)
  local last=$((GUARD_LAST_COMPUTED / 1000))
  if (( last <= 0 )); then
    echo ""
    return
  fi
  local diff=$((now - last))
  if (( diff < 0 )); then
    diff=$(( -diff ))
  fi
  if (( diff < 5 )); then
    echo "just now"
  elif (( diff < 60 )); then
    echo "${diff}s ago"
  elif (( diff < 3600 )); then
    local mins=$((diff / 60))
    echo "${mins}m ago"
  elif (( diff < 86400 )); then
    local hours=$((diff / 3600))
    echo "${hours}h ago"
  else
    local days=$((diff / 86400))
    echo "${days}d ago"
  fi
}

print_guard_status() {
  if [[ $GUARD_STATUS_READY -eq 0 ]]; then
    if [[ $GUARD_REFRESHING -eq 1 ]]; then
      echo "Wallet guard: refreshing..."
    else
      echo "Wallet guard: loading..."
    fi
    return
  fi
  if [[ -z "$GUARD_TOTAL" ]]; then
    echo "Wallet guard: status unavailable"
    return
  fi
  local modeLabel
  if [[ "$GUARD_MODE" == "force-reset" ]]; then
    modeLabel="force reset active (all wallets enabled)"
  else
    modeLabel="auto guard"
  fi
  local refreshTag=""
  if [[ $GUARD_REFRESHING -eq 1 ]]; then
    refreshTag=" (refreshing…)"
  elif [[ $GUARD_REFRESH_NEEDED -eq 1 ]]; then
    refreshTag=" (stale; queued refresh)"
  fi
  echo "Wallet guard: ${GUARD_ACTIVE}/${GUARD_TOTAL} active — ${modeLabel}${refreshTag}"
  local lastUpdate
  lastUpdate=$(describe_last_update)
  if [[ -n "$lastUpdate" ]]; then
    echo "  Last update: $lastUpdate"
  fi
  if [[ "$GUARD_MODE" != "force-reset" && -n "$GUARD_DISABLED" && "$GUARD_DISABLED" != "0" ]]; then
    local sample=""
    if [[ -n "$GUARD_DISABLED_SAMPLE" ]]; then
      sample=" ($(printf '%s' "$GUARD_DISABLED_SAMPLE" | sed 's/,/, /g'))"
    fi
    echo "  Disabled wallets: ${GUARD_DISABLED}${sample}"
  fi
}

print_hotkeys() {
  print_guard_status
  if [[ -z "$WALLET_COUNT" || "$WALLET_COUNT" == "..." ]]; then
    echo "Wallet files tracked: loading..."
  else
    echo "Wallet files tracked: $WALLET_COUNT"
  fi
  echo "Hotkeys:"
  JUPITER_SWAP_TOOL_SKIP_INIT=1 \
  JUPITER_SWAP_TOOL_NO_BANNER=1 \
    node cli_trader.js hotkeys launcher --no-title --indent 2 || \
    echo "  (unable to load hotkey map)"
}

update_launcher_state() {
  load_launcher_state
  if [[ $GUARD_REFRESH_NEEDED -eq 1 ]]; then
    start_guard_refresh
  fi
}

refresh_caches_after_command() {
  update_launcher_state
  print_hotkeys
}

run_cli_command() {
  local description="$1"
  shift
  echo "⏳ $description..."
  "$@"
  local status=$?
  if [[ $status -ne 0 ]]; then
    echo "⚠️ command exited with status $status"
  else
    echo "✅ done."
  fi
  return $status
}

wallet_menu() {
  while true; do
    echo
    echo "Wallet tools:"
    JUPITER_SWAP_TOOL_SKIP_INIT=1 \
    JUPITER_SWAP_TOOL_NO_BANNER=1 \
      node cli_trader.js hotkeys wallet-menu --no-title --indent 2 || \
      echo "  (unable to load wallet hotkeys)"
    read -r -p "wallet> " WALLET_OPT
    WALLET_OPT_LOWER=$(printf '%s' "$WALLET_OPT" | tr '[:upper:]' '[:lower:]')
    case "$WALLET_OPT_LOWER" in
      ""|b)
        break
        ;;
      1)
        run_cli_command "Fetching balances" node cli_trader.js balances
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      2)
        read -p "How many wallets to create? " COUNT
        if ! [[ $COUNT =~ ^[0-9]+$ ]] || [[ $COUNT -le 0 ]]; then
          echo "Invalid count."
          continue
        fi
        read -p "Prefix (default wallet): " PREFIX
        PREFIX=${PREFIX:-wallet}
        JUPITER_SWAP_TOOL_SKIP_INIT=1 \
        JUPITER_SWAP_TOOL_NO_BANNER=1 \
        run_cli_command "Generating $COUNT wallet(s) with prefix \"$PREFIX\"" \
          node cli_trader.js generate "$COUNT" "$PREFIX"
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      3)
        read -r -p "Paste secret key / JSON (single line): " IMPORT_SECRET
        if [[ -z "$IMPORT_SECRET" ]]; then
          echo "No secret provided."
          continue
        fi
        read -r -p "Filename prefix [imported]: " IMPORT_PREFIX
        IMPORT_PREFIX=${IMPORT_PREFIX:-imported}
        JUPITER_SWAP_TOOL_SKIP_INIT=1 \
        JUPITER_SWAP_TOOL_NO_BANNER=1 \
        run_cli_command "Importing secret into ${IMPORT_PREFIX}*" \
          node cli_trader.js import-wallet --secret "$IMPORT_SECRET" --prefix "$IMPORT_PREFIX"
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      4)
        read -r -p "Paste mnemonic phrase: " IMPORT_MNEMONIC
        if [[ -z "$IMPORT_MNEMONIC" ]]; then
          echo "No mnemonic provided."
          continue
        fi
        read -r -p "Filename prefix [imported]: " IMPORT_M_PREFIX
        IMPORT_M_PREFIX=${IMPORT_M_PREFIX:-imported}
        read -r -p "Derivation path [m/44'/501'/0'/0']: " IMPORT_PATH
        read -r -p "Optional passphrase (Enter to skip): " IMPORT_PASSPHRASE
        cmd=(node cli_trader.js import-wallet --secret "$IMPORT_MNEMONIC" --prefix "$IMPORT_M_PREFIX")
        if [[ -n "$IMPORT_PATH" ]]; then
          cmd+=(--path "$IMPORT_PATH")
        fi
        if [[ -n "$IMPORT_PASSPHRASE" ]]; then
          cmd+=(--passphrase "$IMPORT_PASSPHRASE")
        fi
        JUPITER_SWAP_TOOL_SKIP_INIT=1 \
        JUPITER_SWAP_TOOL_NO_BANNER=1 \
        run_cli_command "Importing mnemonic into ${IMPORT_M_PREFIX}*" "${cmd[@]}"
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      5)
        JUPITER_SWAP_TOOL_SKIP_INIT=1 \
        JUPITER_SWAP_TOOL_NO_BANNER=1 \
        run_cli_command "Listing wallets" node cli_trader.js list
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      6)
        run_cli_command "Force resetting wallet guard" node cli_trader.js force-reset-wallets
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      7)
        read -r -p "Wallet to unwrap (# or filename, blank = cancel): " WALLET_UNWRAP
        if [[ -z "$WALLET_UNWRAP" ]]; then
          echo "Cancelled."
          continue
        fi
        read -r -p "Amount to unwrap (decimal or 'all', blank = all): " WALLET_UNWRAP_AMT
        WALLET_UNWRAP_AMT=${WALLET_UNWRAP_AMT:-all}
        run_cli_command "Unwrapping wSOL" node cli_trader.js wallet unwrap "$WALLET_UNWRAP" "$WALLET_UNWRAP_AMT"
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      8)
        JUPITER_SWAP_TOOL_SKIP_INIT=1 \
        JUPITER_SWAP_TOOL_NO_BANNER=1 \
        run_cli_command "Fund wallet" node cli_trader.js wallet fund
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      9)
        read -r -p "Anchor wallet for redistribution (# or filename, blank = #1): " WALLET_REDISTRIBUTE
        cmd=(node cli_trader.js wallet redistribute)
        if [[ -n "$WALLET_REDISTRIBUTE" ]]; then
          cmd+=("$WALLET_REDISTRIBUTE")
        fi
        run_cli_command "Redistributing SOL" "${cmd[@]}"
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      0)
        read -r -p "Aggregate toward wallet (# or filename, blank = #1): " WALLET_AGGREGATE
        cmd=(node cli_trader.js wallet aggregate)
        if [[ -n "$WALLET_AGGREGATE" ]]; then
          cmd+=("$WALLET_AGGREGATE")
        fi
        run_cli_command "Aggregating SOL" "${cmd[@]}"
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      *)
        echo "Unknown option: $WALLET_OPT"
        ;;
    esac
  done
}

rpc_tests_menu() {
  while true; do
    echo
    echo "RPC endpoint diagnostics:"
    JUPITER_SWAP_TOOL_SKIP_INIT=1 \
    JUPITER_SWAP_TOOL_NO_BANNER=1 \
      node cli_trader.js hotkeys rpc-tests-menu --no-title --indent 2 || \
      echo "  (unable to load RPC test hotkeys)"
    read -r -p "rpc-test option> " RPC_OPT
    RPC_OPT_LOWER=$(printf '%s' "$RPC_OPT" | tr '[:upper:]' '[:lower:]')
    case "$RPC_OPT_LOWER" in
      ""|b)
        break
        ;;
      1)
        run_cli_command "RPC test: all endpoints" node cli_trader.js test-rpcs all
        update_launcher_state
        ;;
      2)
        read -r -p "Enter 1-based index: " RPC_INDEX
        if [[ ! "$RPC_INDEX" =~ ^[0-9]+$ ]] || [[ "$RPC_INDEX" -le 0 ]]; then
          echo "Invalid index."
          continue
        fi
        run_cli_command "RPC test: index $RPC_INDEX" node cli_trader.js test-rpcs "$RPC_INDEX"
        update_launcher_state
        ;;
      3)
        read -r -p "Substring to match: " RPC_MATCH
        if [[ -z "$RPC_MATCH" ]]; then
          echo "No substring provided."
          continue
        fi
        run_cli_command "RPC test: match \"$RPC_MATCH\"" node cli_trader.js test-rpcs "$RPC_MATCH"
        update_launcher_state
        ;;
      4)
        read -r -p "Full RPC URL: " RPC_URL_CUSTOM
        if [[ -z "$RPC_URL_CUSTOM" ]]; then
          echo "No URL provided."
          continue
        fi
        run_cli_command "RPC test: custom URL" node cli_trader.js test-rpcs "$RPC_URL_CUSTOM"
        update_launcher_state
        ;;
      5)
        read -r -p "Amount in SOL per round [0.001]: " RPC_SWAP_AMOUNT
        RPC_SWAP_AMOUNT=${RPC_SWAP_AMOUNT:-0.001}
        read -r -p "Number of rounds [10]: " RPC_SWAP_LOOPS
        RPC_SWAP_LOOPS=${RPC_SWAP_LOOPS:-10}
        read -r -p "Delay between rounds in ms [1000]: " RPC_SWAP_DELAY
        RPC_SWAP_DELAY=${RPC_SWAP_DELAY:-1000}
        read -r -p "This will perform live SOL→USDC→SOL swaps. Continue? (y/N): " RPC_SWAP_CONFIRM
        RPC_SWAP_CONFIRM=$(printf '%s' "$RPC_SWAP_CONFIRM" | tr '[:upper:]' '[:lower:]')
        if [[ "$RPC_SWAP_CONFIRM" != "y" && "$RPC_SWAP_CONFIRM" != "yes" ]]; then
          echo "Cancelled swap stress test."
          continue
        fi
        run_cli_command "RPC swap stress test" \
          node cli_trader.js test-rpcs all --swap --confirm --amount "$RPC_SWAP_AMOUNT" --loops "$RPC_SWAP_LOOPS" --delay "$RPC_SWAP_DELAY"
        update_launcher_state
        ;;
      *)
        echo "Unknown option: $RPC_OPT"
        ;;
    esac
    read -p "Press Enter to continue..." _
  done
}

test_menu() {
  while true; do
    echo
    echo "Test utilities:"
    JUPITER_SWAP_TOOL_SKIP_INIT=1 \
    JUPITER_SWAP_TOOL_NO_BANNER=1 \
      node cli_trader.js hotkeys test-menu --no-title --indent 2 || \
      echo "  (unable to load test hotkeys)"
    read -r -p "test> " TEST_OPT
    TEST_OPT_LOWER=$(printf '%s' "$TEST_OPT" | tr '[:upper:]' '[:lower:]')
    case "$TEST_OPT_LOWER" in
      ""|b)
        break
        ;;
      1)
        rpc_tests_menu
        ;;
      2)
        echo "Leave fields blank to use defaults (crew_1.json, SOL → USDC, 0.001 SOL, dry run)."
        read -r -p "Wallet file: " TEST_WALLET
        read -r -p "Input mint or symbol: " TEST_INPUT
        read -r -p "Output mint or symbol: " TEST_OUTPUT
        read -r -p "Amount (decimal SOL units): " TEST_AMOUNT
        read -r -p "Submit signed swap on-chain? (y/N): " TEST_SUBMIT
        TEST_SUBMIT=$(printf '%s' "$TEST_SUBMIT" | tr '[:upper:]' '[:lower:]')
        cmd=(node cli_trader.js test-ultra)
        if [[ -n "$TEST_WALLET" ]]; then
          cmd+=(--wallet "$TEST_WALLET")
        fi
        if [[ -n "$TEST_INPUT" ]]; then
          cmd+=(--input "$TEST_INPUT")
        fi
        if [[ -n "$TEST_OUTPUT" ]]; then
          cmd+=(--output "$TEST_OUTPUT")
        fi
        if [[ -n "$TEST_AMOUNT" ]]; then
          cmd+=(--amount "$TEST_AMOUNT")
        fi
        if [[ "$TEST_SUBMIT" == "y" || "$TEST_SUBMIT" == "yes" ]]; then
          cmd+=(--submit)
        fi
        run_cli_command "Ultra API swap test" "${cmd[@]}"
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      *)
        echo "Unknown option: $TEST_OPT"
        ;;
    esac
  done
}

flows_menu() {
  while true; do
    echo
    echo "Trading flows:"
    JUPITER_SWAP_TOOL_SKIP_INIT=1 \
    JUPITER_SWAP_TOOL_NO_BANNER=1 \
      node cli_trader.js hotkeys flows-menu --no-title --indent 2 || \
      echo "  (unable to load flows hotkeys)"
    read -r -p "flow> " FLOW_OPT
    FLOW_OPT_LOWER=$(printf '%s' "$FLOW_OPT" | tr '[:upper:]' '[:lower:]')
    case "$FLOW_OPT_LOWER" in
      ""|b)
        break
        ;;
      1|a)
        run_cli_command "Arpeggio flow (15min)" node cli_trader.js arpeggio
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      2|h)
        run_cli_command "Horizon flow (60min)" node cli_trader.js horizon
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      3|e)
        run_cli_command "Echo flow (6hr)" node cli_trader.js echo
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      4|i)
        run_cli_command "Icarus flow (high-tempo random)" node cli_trader.js icarus
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      5|z)
        run_cli_command "Zenith flow (mid-tempo random)" node cli_trader.js zenith
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      6|u)
        run_cli_command "Aurora flow (slow steady random)" node cli_trader.js aurora
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      7|t)
        run_cli_command "Titan whale flow (aggressive)" node cli_trader.js titan
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      8|o)
        run_cli_command "Odyssey whale flow (diverse)" node cli_trader.js odyssey
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      9|v)
        run_cli_command "Sovereign whale flow (strategic)" node cli_trader.js sovereign
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      *)
        echo "Unknown option: $FLOW_OPT"
        ;;
    esac
  done
}

lend_menu() {
  while true; do
    echo
    echo "Jupiter Lend (earn beta — borrow coming soon):"
    JUPITER_SWAP_TOOL_SKIP_INIT=1 \
    JUPITER_SWAP_TOOL_NO_BANNER=1 \
      node cli_trader.js hotkeys lend-menu --no-title --indent 2 || \
      echo "  (unable to load lend hotkeys)"
    read -r -p "lend> " LEND_OPT
    LEND_OPT_LOWER=$(printf '%s' "$LEND_OPT" | tr '[:upper:]' '[:lower:]')
    case "$LEND_OPT_LOWER" in
      ""|b)
        break
        ;;
      1)
        run_cli_command "Lend earn tokens" node cli_trader.js lend earn tokens --refresh
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      2)
        echo "Leave fields blank to target every active wallet/token and auto-max amount."
        read -r -p "Wallet (# or filename, blank = all wallets): " LEND_WALLET
        read -r -p "Deposit mint or symbol (blank = eligible tokens): " LEND_MINT
        read -r -p "Amount (decimal, blank = max spendable): " LEND_AMOUNT
        if [[ -z "$LEND_WALLET" ]]; then LEND_WALLET="*"; fi
        if [[ -z "$LEND_MINT" ]]; then LEND_MINT="*"; fi
        if [[ -z "$LEND_AMOUNT" ]]; then LEND_AMOUNT="*"; fi
        run_cli_command "Lend earn deposit" node cli_trader.js lend earn deposit "$LEND_WALLET" "$LEND_MINT" "$LEND_AMOUNT"
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      3)
        echo "Leave fields blank to auto-select share tokens and withdraw full balance."
        read -r -p "Wallet (# or filename, blank = all wallets): " LEND_WALLET
        read -r -p "Withdraw mint or symbol (blank = share tokens): " LEND_MINT
        read -r -p "Amount (decimal, blank = max available): " LEND_AMOUNT
        if [[ -z "$LEND_WALLET" ]]; then LEND_WALLET="*"; fi
        if [[ -z "$LEND_MINT" ]]; then LEND_MINT="*"; fi
        if [[ -z "$LEND_AMOUNT" ]]; then LEND_AMOUNT="*"; fi
        run_cli_command "Lend earn withdraw" node cli_trader.js lend earn withdraw "$LEND_WALLET" "$LEND_MINT" "$LEND_AMOUNT"
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      4)
        read -r -p "Wallet identifiers (#, filename, or pubkey; blank = all wallets): " LEND_IDS
        if [[ -z "$LEND_IDS" ]]; then
          LEND_IDS="*"
        fi
        run_cli_command "Lend earn positions" node cli_trader.js lend earn positions "$LEND_IDS"
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      5)
        read -r -p "Wallet identifiers (#, filename, or pubkey; blank = all wallets): " LEND_IDS
        if [[ -z "$LEND_IDS" ]]; then
          LEND_IDS="*"
        fi
        run_cli_command "Lend earn earnings" node cli_trader.js lend earn earnings "$LEND_IDS"
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      6)
        run_cli_command "Lend overview (earn)" node cli_trader.js lend overview
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      *)
        echo "Borrow tooling is coming soon — choose 1-6 or 'b' to exit."
        ;;
    esac
  done
}

advanced_menu() {
  while true; do
    echo
    echo "Advanced trade tools:"
    ADVANCED_OUTPUT=$(
      JUPITER_SWAP_TOOL_SKIP_INIT=1 \
      JUPITER_SWAP_TOOL_NO_BANNER=1 \
        node cli_trader.js hotkeys advanced-menu --no-title --indent 2 2>/dev/null \
        || true
    )
    if [[ -n "$ADVANCED_OUTPUT" ]]; then
      printf "%s\n" "$ADVANCED_OUTPUT"
    else
      echo "  (unable to load advanced hotkeys)"
    fi
    FLOW_SHORTCUTS_OUTPUT=$(
      JUPITER_SWAP_TOOL_SKIP_INIT=1 \
      JUPITER_SWAP_TOOL_NO_BANNER=1 \
        node cli_trader.js hotkeys flow-shortcuts --no-title --indent 4 2>/dev/null \
        || true
    )
    if [[ -n "$FLOW_SHORTCUTS_OUTPUT" ]]; then
      echo "    Flow shortcuts:"
      printf "%s\n" "$FLOW_SHORTCUTS_OUTPUT"
    fi
    read -r -p "advanced> " ADV_OPT
    ADV_OPT_LOWER=$(printf '%s' "$ADV_OPT" | tr '[:upper:]' '[:lower:]')
    case "$ADV_OPT_LOWER" in
      ""|b)
        break
        ;;
      1)
        read -p "Starting mint (press Enter for SOL): " START_MINT
        if [[ -n "$START_MINT" ]]; then
          run_cli_command "Target loop starting at ${START_MINT}" node cli_trader.js target-loop "$START_MINT"
        else
          run_cli_command "Target loop starting at SOL" node cli_trader.js target-loop
        fi
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      2)
        read -p "Enable extra random sweep after long circle? (y/N): " EXTRA_RANDOM
        EXTRA_RANDOM=$(printf '%s' "$EXTRA_RANDOM" | tr '[:upper:]' '[:lower:]')
        if [[ "$EXTRA_RANDOM" == "y" || "$EXTRA_RANDOM" == "yes" ]]; then
          run_cli_command "Long circle swap with extra sweep" node cli_trader.js long-circle --extra
        else
          run_cli_command "Long circle swap (primary path)" node cli_trader.js long-circle --primary-only
        fi
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      3)
        test_menu
        update_launcher_state
        ;;
      4)
        node cli_trader.js crew1-cycle
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      5)
        run_cli_command "Sweep balances into wBTC / cbBTC / wETH" node cli_trader.js sweep-to-btc-eth
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      6)
        run_cli_command "SOL → USDC → POPCAT lap" node cli_trader.js sol-usdc-popcat
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      7)
        lend_menu
        update_launcher_state
        ;;
      8|flows)
        flows_menu
        update_launcher_state
        ;;
      a)
        run_cli_command "Arpeggio flow (fast deterministic)" node cli_trader.js arpeggio
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      i)
        run_cli_command "Icarus flow (high-tempo random)" node cli_trader.js icarus
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      z)
        run_cli_command "Zenith flow (mid-tempo diverse)" node cli_trader.js zenith
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      u)
        run_cli_command "Aurora flow (slow extended)" node cli_trader.js aurora
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      t)
        run_cli_command "Titan whale flow (aggressive $5+)" node cli_trader.js titan
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      o)
        run_cli_command "Odyssey whale flow (diverse)" node cli_trader.js odyssey
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      v)
        run_cli_command "Sovereign whale flow (strategic)" node cli_trader.js sovereign
        read -p "Press Enter to continue..." _
        update_launcher_state
        ;;
      *)
        echo "Unknown option: $ADV_OPT"
        ;;
    esac
  done
}

USDC_MINT="${USDC_MINT:-$USDC_MAINNET}"
reset_launcher_state
update_launcher_state
KEYPAIRS_EXISTS_NOW=0
if [[ -d "$KEYPAIRS_DIR" ]]; then
  KEYPAIRS_EXISTS_NOW=1
  if [[ $KEYPAIRS_EXISTED_BEFORE -eq 1 ]]; then
    echo "Keypair directory ready at ${KEYPAIRS_DIR}/"
  else
    echo "Initialized keypair directory at ${KEYPAIRS_DIR}/"
  fi
fi
RPC_FILE_EXISTS_NOW=0
if [[ -f "$RPC_FILE_CONFIG" ]]; then
  RPC_FILE_EXISTS_NOW=1
  if [[ $RPC_FILE_EXISTED_BEFORE -eq 1 ]]; then
    echo "RPC endpoints file found at ${RPC_FILE_CONFIG}"
  else
    echo "Created RPC endpoints template at ${RPC_FILE_CONFIG}"
  fi
fi
print_hotkeys

while true; do
  reap_guard_refresh
  echo
  read -r -p "cli_trader> " CMD
  if [[ -z "$CMD" ]]; then
    echo "No command entered. Press 0 to quit or choose another hotkey."
    print_hotkeys
    continue
  fi

  EXEC_DESC=""
  EXEC_ARGS=()
  CMD_LOWER=$(printf '%s' "$CMD" | tr '[:upper:]' '[:lower:]')

  case "$CMD_LOWER" in
    1|w)
      wallet_menu
      refresh_caches_after_command
      continue
      ;;
    2|g|forcereset|force-reset|reset)
      run_cli_command "Force resetting wallet guard" node cli_trader.js force-reset-wallets
      refresh_caches_after_command
      continue
      ;;
    3|d|redistribute)
      run_cli_command "Redistribute $CREW_WALLET" node cli_trader.js redistribute "$CREW_WALLET"
      refresh_caches_after_command
      continue
      ;;
    4|a|aggregate)
      run_cli_command "Aggregate into $CREW_WALLET" node cli_trader.js aggregate "$CREW_WALLET"
      refresh_caches_after_command
      continue
      ;;
    5|c|reclaim|close|close-token-accounts|reclaimsol)
      run_cli_command "Reclaim SOL (close empty token accounts)" node cli_trader.js reclaim-sol
      refresh_caches_after_command
      continue
      ;;
    6|u|sol2usdc|swap-sol-usdc)
      EXEC_DESC="Swap SOL -> USDC (default amount)"
      EXEC_ARGS=(swap "$SOL_MINT" "$USDC_MINT")
      ;;
    7|b|buckshot)
      EXEC_DESC="Buckshot mode (interactive rotation)"
      EXEC_ARGS=(buckshot)
      ;;
    8|s|sweep-all|sweepall|sweep)
      EXEC_DESC="Sweep all token balances -> SOL"
      EXEC_ARGS=(sweep-all)
      ;;
    t|test|tests)
      test_menu
      refresh_caches_after_command
      continue
      ;;
    9|v|advanced)
      advanced_menu
      refresh_caches_after_command
      continue
      ;;
    0|q|quit|exit)
      break
      ;;
    *)
      EXEC_DESC="cli_trader.js $CMD"
      IFS=' ' read -r -a EXEC_ARGS <<< "$CMD"
      ;;
  esac

  if [[ ${#EXEC_ARGS[@]} -gt 0 ]]; then
    run_cli_command "$EXEC_DESC" node cli_trader.js "${EXEC_ARGS[@]}"
    refresh_caches_after_command
    continue
  fi
done

echo
while true; do
  reap_guard_refresh
  read -r -p "Type CLOSE (then Enter) to close this window: " CLOSE_INPUT
  CLOSE_INPUT=$(printf '%s' "$CLOSE_INPUT" | tr '[:lower:]' '[:upper:]')
  if [[ "$CLOSE_INPUT" == "CLOSE" ]]; then
    break
  fi
done
