# Shadowrun (local development)

This repo contains tooling and a C# local service that lets the Shadowrun: Chronicles client talk to a local server.

## Prerequisites

- Windows + PowerShell
- A local install of **Shadowrun: Chronicles** (Steam)
- **.NET Framework 3.5** (needed for `MSBuild.exe` used by the C# service)
  - Install/enable steps (Microsoft docs): https://learn.microsoft.com/dotnet/framework/install/dotnet-35-windows

## 1) Clone

```powershell
git clone https://github.com/bradleyholloway/bostonunlocked
cd shadowrun
```

## 2) Extract required game resources

The local server needs a few DLLs, static-data JSON, and StreamingAssets copied out of your game install.

Run:

```powershell
powershell -ExecutionPolicy Bypass -File .\extractresourcesfrominstallation.ps1 -GameRoot "D:\SteamLibrary\steamapps\common\ShadowrunChronicles" -InstallPythonDeps
```

This writes into:

- `server/src/Dependencies/` (DLLs)
- `server/static-data/` (JSON)
- `server/StreamingAssets/` (copied folder)

If you move/reinstall the game, re-run the extractor.

## 3) Patch the client to point at your server

The game client reads its endpoint hostnames from:

- `<GameRoot>\Shadowrun_Data\resources.assets`

This repo ships a patch tool executable:

- `clientsetup/patch_embedded_configs.exe`

### Patch for local server (127.0.0.1)

```powershell
.\clientsetup\patch_embedded_configs.exe --asset "D:\SteamLibrary\steamapps\common\ShadowrunChronicles\Shadowrun_Data\resources.assets" patch --host 127.0.0.1
```

Notes:

- The tool creates a backup next to the asset (by default `resources.assets.bak`).
- `--asset` is a global flag, so it must come **before** `patch`/`restore`.

### Restore to connect to the normal/online servers again

If you patched to `127.0.0.1` and want to revert:

```powershell
.\clientsetup\patch_embedded_configs.exe --asset "D:\SteamLibrary\steamapps\common\ShadowrunChronicles\Shadowrun_Data\resources.assets" restore
```

Help:

```powershell
.\clientsetup\patch_embedded_configs.exe --help
.\clientsetup\patch_embedded_configs.exe patch --help
```

## 4) Run the local server

Start the server (builds the C# host and launches it):

```powershell
powershell -ExecutionPolicy Bypass -File .\server\start_localserver.ps1
```

Defaults:

- HTTP bind host: `0.0.0.0`
- HTTP port: `80`
- APlay port: `5055`
- Photon port: `4530`

If port 80 fails to bind, run your terminal as Administrator.

## Resetting server progress/state

The server persists state under:

- `server/data/`

To reset all server progress, stop the server and delete that folder:

```powershell
Remove-Item -Recurse -Force .\server\data
```
