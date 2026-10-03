; Reproducible Windows x64 installer. AppVersion is updated only in the final release PR.
#define AppName "PowerSim"
#define AppVersion "1.0.0-dev"
#define AppPublisher "PowerSim"
#define AppExeName "PowerSim.exe"

[Setup]
AppId={{E2D4C5DD-43A0-4C03-920F-5C0E15B7B5D5}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
OutputDir=..\..\dist\installer
OutputBaseFilename=PowerSim-{#AppVersion}-Windows-x64-Setup
Compression=lzma2
SolidCompression=yes
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
UninstallDisplayName={#AppName}

[Files]
Source: "..\..\dist\PowerSim\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\PowerSim"; Filename: "{app}\{#AppExeName}"
Name: "{autodesktop}\PowerSim"; Filename: "{app}\{#AppExeName}"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; GroupDescription: "Additional shortcuts:"

[Run]
Filename: "{app}\{#AppExeName}"; Description: "Launch PowerSim"; Flags: nowait postinstall skipifsilent
