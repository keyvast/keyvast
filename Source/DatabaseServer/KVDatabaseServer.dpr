{ KeyVast Database Server }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ Icon from Kameleon Icons http://www.kameleon.pics/ }

{ 2018/02/19  0.01  Initial version  }
{ 2018/02/25  0.02  Support for Linux and OSX }


program KVDatabaseServer;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  {$IFDEF MSWINDOWS}
  Windows,
  {$ENDIF }
  {$IFDEF POSIX}
  Posix.Unistd,
  {$ENDIF }
  {$IFDEF MACOS}
  Macapi.CoreFoundation,
  {$ENDIF }
  IOUtils,
  System.SysUtils,
  SyncObjs,
  Classes,
  IniFiles,
  kvHashList in '..\KVS\kvHashList.pas',
  kvHash in '..\KVS\kvHash.pas',
  kvValues in '..\KVS\kvValues.pas',
  kvStructures in '..\KVS\kvStructures.pas',
  kvFiles in '..\KVS\kvFiles.pas',
  kvObjects in '..\KVS\kvObjects.pas',
  kvScriptContext in '..\KVS\kvScriptContext.pas',
  kvScriptNodes in '..\KVS\kvScriptNodes.pas',
  kvScriptParser in '..\KVS\kvScriptParser.pas',
  kvScriptSystem in '..\KVS\kvScriptSystem.pas',
  kvTest in '..\KVS\kvTest.pas',
  kvdsApplication in 'kvdsApplication.pas',
  kvScriptFunctions in '..\KVS\kvScriptFunctions.pas';

type
  EAppError = class(Exception);

var
  ParamHelp : Boolean = False;
  ParamPath : String;
  ParamName : String;
  ParamNoLogDisplay : Boolean = False;
  ParamNoLogFile : Boolean = False;
  ParamLogCommands : Boolean = False;
  ParamDaemon : Boolean = False;
  ParamTcpPort : Integer = -1;
  AppTerminated : Boolean = False;

function GetDefaultIniPath: String;
begin
  Result := TPath.GetHomePath;
  {$IFDEF MSWINDOWS}
  Result := Result + '\kvds\';
  {$ENDIF}
  {$IFDEF POSIX}
  Result := Result + '/.kvds/';
  {$ENDIF}
end;

procedure ReadIniFile;
var
  Ini : TMemIniFile;
begin
  Ini := TMemIniFile.Create(GetDefaultIniPath + 'kvds.ini');
  try
    ParamPath := Ini.ReadString('System', 'Path', '');
    ParamName := Ini.ReadString('System', 'Name', '');
    ParamTcpPort := Ini.ReadInteger('System', 'TcpPort', -1);
  finally
    Ini.Free;
  end;
end;

procedure ParseParameters;
var
  I : Integer;
  P : String;
  S, T, U : String;
  ParamNr : Integer;
  J : Integer;
begin
  ParamNr := 0;
  for I := 1 to ParamCount do
    begin
      P := ParamStr(I);
      S := Trim(P);
      if S.StartsWith('--') then
        begin
          S := S.Remove(0, 2);
          S := S.ToLower;
          if S = 'help' then
            ParamHelp := True
          else
          if S = 'nologdisplay' then
            ParamNoLogDisplay := True
          else
          if S = 'nologfile' then
            ParamNoLogFile := True
          else
          if S = 'logcommands' then
            ParamLogCommands := True
          else
          if S = 'daemon' then
            ParamDaemon := True
          else
            begin
              J := S.IndexOf('=');
              if J >= 0 then
                begin
                  T := S.Substring(0, J).Trim;
                  U := S.Substring(J + 1, Length(S) - J).Trim;
                  if T = 'tcpport' then
                    begin
                      if not TryStrToInt(U, ParamTcpPort) then
                        raise EAppError.CreateFmt('Invalid tcp port: %s', [U]);
                      if ParamTcpPort > $FFFF then
                        raise EAppError.CreateFmt('Invalid tcp port: %d', [ParamTcpPort]);
                    end
                  else
                    raise EAppError.CreateFmt('Unknown parameter: %s', [T])
                end
              else
                raise EAppError.CreateFmt('Unknown parameter: %s', [P])
            end;
        end
      else
      if S.StartsWith('-') then
        begin
          S := S.Remove(0, 1);
          S := S.ToLower;
          if S = 'h' then
            ParamHelp := True
          else
            raise EAppError.CreateFmt('Unknown parameter: %s', [P])
        end
      else
        begin
          Inc(ParamNr);
          if ParamNr = 1 then
            ParamPath := S
          else
          if ParamNr = 2 then
            ParamName := S
          else
            raise EAppError.Create('Too many parameters');
        end;
    end;
  if (ParamPath = '') or (ParamName = '') then
    if ParamNr = 0 then
      ParamHelp := True
    else
    if ParamNr < 2 then
      raise EAppError.Create('Missing parameter');
end;

procedure PrintTitle;
begin
  Writeln('KeyVast Database Server 1.10');
end;

procedure PrintHelp;
begin
  PrintTitle;
  Writeln;
  Writeln('Usage:');
  Writeln('  KVDatabaseServer [<options>] <path> <name>');
  Writeln;
  Writeln('Options:');
  Writeln('  -h --help         Print help message');
  Writeln('  --daemon          Run as daemon (Posix only)');
  Writeln('  --nologdisplay    Don''t log to console');
  Writeln('  --nologfile       Don''t log to file');
  Writeln('  --logcommands     Log individual commands and responses');
  Writeln('  --tcpport=<port>  TCP port');
end;

function GetDefaultLogPath: String;
begin
  Result := TPath.GetHomePath;
  {$IFDEF MSWINDOWS}
  Result := Result + '\log\';
  {$ENDIF}
  {$IFDEF POSIX}
  Result := Result + '/log/';
  {$ENDIF}
end;

{ TAppServices }

type
  TAppServices = class
    FLock : TCriticalSection;
    FLogPath : String;
    FLogFileName : String;
    constructor Create;
    destructor Destroy; override;
    procedure Lock;
    procedure Unlock;
    procedure AppendLogFile(const LogS: String);
    procedure AppLog(LogType: Char; LogMsg: String; LogLevel: Integer);
  end;

var
  AppServices : TAppServices = nil;

constructor TAppServices.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;

  FLogPath := GetDefaultLogPath;
  if (FLogPath <> '') and not ParamNoLogFile then
    ForceDirectories(FLogPath);

  FLogFileName := FLogPath + 'kvsdbs.log';
end;

destructor TAppServices.Destroy;
begin
  FreeAndNil(FLock);
  inherited Destroy;
end;

procedure TAppServices.Lock;
begin
  if Assigned(FLock) then
    FLock.Acquire;
end;

procedure TAppServices.Unlock;
begin
  if Assigned(FLock) then
    FLock.Release;
end;

procedure TAppServices.AppendLogFile(const LogS: String);
var
  LogFS : String;
  LogUtf : RawByteString;
  NewFile : Boolean;
  FileMode : Word;
  LogFile : TFileStream;
begin
  try
    LogFS := LogS + #13#10;
    LogUtf := UTF8Encode(LogFS);
    NewFile := not FileExists(FLogFileName);
    if NewFile then
      FileMode := fmCreate
    else
      FileMode := fmOpenReadWrite;
    FileMode := FileMode or fmShareDenyWrite;
    LogFile := TFileStream.Create(FLogFileName, FileMode);
    try
      if not NewFile then
        LogFile.Seek(0, soEnd);
      LogFile.Write(LogUtf[1], Length(LogUtf));
    finally
      LogFile.Free;
    end;
  except
  end;
end;

procedure TAppServices.AppLog(LogType: Char; LogMsg: String; LogLevel: Integer);
var
  LogToConsole : Boolean;
  LogToFile : Boolean;
  LogS : String;
begin
  LogToConsole := not ParamNoLogDisplay and not ParamDaemon;
  LogToFile := not ParamNoLogFile;
  if LogToConsole or LogToFile then
    begin
      LogS := FormatDateTime('hh:nn:ss.zzz', Now) + ' ' + LogType + ' ' +
              IntToStr(LogLevel) + ' ' + LogMsg;
      Lock;
      try
        if LogToConsole then
          Writeln(LogS);
        if LogToFile then
          AppendLogFile(LogS);
      finally
        Unlock;
      end;
    end;
end;

procedure AppLock;
begin
  if Assigned(AppServices) then
    AppServices.Lock;
end;

procedure AppUnlock;
begin
  if Assigned(AppServices) then
    AppServices.Unlock;
end;

{$IFDEF MSWINDOWS}
function ConsoleCtrlHandlerRoutine(const dwCtrlType: LongWord): Boolean; stdcall;
begin
  AppLock;
  AppTerminated := True;
  AppUnlock;
  Result := True;
end;
{$ENDIF}

begin
  {$IFDEF TEST}
  kvTest.Test;
  {$ENDIF}
  try
    ReadIniFile;
    ParseParameters;
    if ParamHelp then
      begin
        PrintHelp;
        exit;
      end;
    {$IFNDEF POSIX}
    if ParamDaemon then
      raise EAppError.Create('Daemon mode only available on Posix systems');
    {$ENDIF}
    {$IFDEF POSIX}
    if ParamDaemon then
      if fork <> 0 then
        exit;
    {$ENDIF}
    AppServices := TAppServices.Create;
    App := TkvdsApplication.Create;
    try
      {$IFDEF MSWINDOWS}
      SetConsoleCtrlHandler(@ConsoleCtrlHandlerRoutine, True);
      {$ENDIF}
      if not ParamDaemon then
        PrintTitle;
      App.LogCommands := ParamLogCommands and not ParamDaemon;
      App.OnLog := AppServices.AppLog;
      App.SysPath := ParamPath;
      App.SysName := ParamName;
      if ParamTcpPort > 0 then
        App.TcpPort := ParamTcpPort;
      App.Start;
      repeat
        AppServices.Lock;
        try
          if AppTerminated then
            break;
        finally
          AppServices.Unlock;
        end;
        if App.Terminated then
          break;
        App.Process;
      until false;
      App.Stop;
    finally
      FreeAndNil(App);
      FreeAndNil(AppServices);
    end;
  except
    on E: Exception do
      Writeln('Error: ', E.ClassName, ': ', E.Message);
  end;
end.

