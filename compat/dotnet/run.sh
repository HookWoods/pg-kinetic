#!/usr/bin/env bash
set -euo pipefail

cd "$(cd "$(dirname "$0")/../.." && pwd -P)"

if ! command -v dotnet >/dev/null 2>&1; then
  printf '{"ok":true,"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"toolchain-unavailable","language":"dotnet","error_summary":"dotnet is unavailable"}\n'
  exit 0
fi

if ! dotnet --list-sdks | grep -Eq '(^|[[:space:]])8\.'; then
  printf '{"ok":true,"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"toolchain-unavailable","language":"dotnet","error_summary":".NET 8 SDK is required"}\n'
  exit 0
fi

if [ ! -f compat/dotnet/obj/project.assets.json ] && ! dotnet restore compat/dotnet/PgKinetic.Compatibility.csproj --ignore-failed-sources --nologo >/dev/null 2>&1; then
  printf '{"ok":true,"success_marker":"compatibility report complete","outcome":"skip","skip_reason":"library-unavailable","language":"dotnet","error_summary":"Npgsql packages could not be restored"}\n'
  exit 0
fi

dotnet run --no-restore --project compat/dotnet/PgKinetic.Compatibility.csproj --nologo
