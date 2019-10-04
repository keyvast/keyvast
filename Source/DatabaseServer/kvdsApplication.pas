{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/19  0.01  Initial version, basic script execution }

unit kvdsApplication;

interface

uses
  SysUtils,
  SyncObjs,
  kvDiskSystem,
  kvScriptParser,
  kvScriptSystem,
  kvDatasysServer,
  IdContext,
  IdTCPServer;



const
  TCP_DefaultPort = 7950;

type
  EkvdsApplication = class(Exception);

  TkvdsApplicationLogEvent = procedure (LogType: Char; LogMsg: String;
      LogLevel: Integer) of object;

  TkvdsApplication = class
  private
    FLock        : TCriticalSection;
    FOnLog       : TkvdsApplicationLogEvent;
    FLogCommands : Boolean;
    FSysPath     : String;
    FSysName     : String;
    FTcpPort     : Integer;

    FTerminated    : Boolean;
    FDatasysServer : TkvDatasysServer;
    FSys           : TkvSystem;
    FMSys          : TkvScriptSystem;

    procedure Lock;
    procedure Unlock;

    function  GetTerminated: Boolean;

    procedure Log(const LogType: Char; const LogMsg: String;
              const LogLevel: Integer = 0); overload;
    procedure Log(const LogType: Char; const LogMsg: String;
              const Args: array of const; const LogLevel: Integer = 0); overload;

    procedure DatasysServerLog(LogType: Char; LogMsg: String;
              LogLevel: Integer);
    procedure DatasysServerTextCommand(Server: TkvDatasysServer; const CmdText: String);
    procedure DatasysServerStopCommand(Server: TkvDatasysServer);
    procedure DatasysServerTextResponse(Server: TkvDatasysServer; const RespText: String);

  public
    constructor Create;
    destructor Destroy; override;

    property  OnLog: TkvdsApplicationLogEvent read FOnLog write FOnLog;
    property  LogCommands: Boolean read FLogCommands write FLogCommands;

    property  SysPath: String read FSysPath write FSysPath;
    property  SysName: String read FSysName write FSysName;

    property  TcpPort: Integer read FTcpPort write FTcpPort;

    procedure Start;
    procedure Stop;
    procedure Process;
    property  Terminated: Boolean read GetTerminated;
  end;



var
  App : TkvdsApplication = nil;



implementation

uses
  IdGlobal,
  kvValues;


{$IFDEF LINUX}
  {$DEFINE IdContextUseDataObject}
{$ENDIF}



{ Utilities }

const
  WideCRLF = String(WideChar(#13) + WideChar(#10));

function kvLogTextClean(const S: String): String;
var
  T : String;
begin
  T := StringReplace(S, #13, ' ', [rfReplaceAll]);
  T := StringReplace(T, #10, ' ', [rfReplaceAll]);
  if Length(T) > 64 then
    T := T.Remove(64);
  Result := T;
end;



{ TkvdsApplication }

constructor TkvdsApplication.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FLogCommands := True;
  FTerminated := False;
  FTcpPort := TCP_DefaultPort;
  FDatasysServer := TkvDatasysServer.Create(TCP_DefaultPort);
  FDatasysServer.OnLog := DatasysServerLog;
  FDatasysServer.OnTextCommand := DatasysServerTextCommand;
  FDatasysServer.OnTextResponse := DatasysServerTextResponse;
  FDatasysServer.OnStopCommand := DatasysServerStopCommand;
end;

destructor TkvdsApplication.Destroy;
begin
  FreeAndNil(FDatasysServer);
  FreeAndNil(FMSys);
  FreeAndNil(FSys);
  FreeAndNil(FLock);
  inherited Destroy;
end;

procedure TkvdsApplication.Lock;
begin
  if Assigned(FLock) then
    FLock.Acquire;
end;

procedure TkvdsApplication.Unlock;
begin
  if Assigned(FLock) then
    FLock.Release;
end;

function TkvdsApplication.GetTerminated: Boolean;
begin
  Lock;
  Result := FTerminated;
  Unlock;
end;

procedure TkvdsApplication.Start;
begin
  Log('#', 'Starting');
  try
    if FSysName = '' then
      raise EkvdsApplication.Create('System name required');

    Log('#', 'System path: %s', [FSysPath]);
    Log('#', 'System name: %s', [FSysName]);

    FSys := TkvSystem.Create(FSysPath, FSysName);
    FMSys := TkvScriptSystem.Create(FSys);
    if FSys.Exists then
      begin
        FSys.Open;
        FMSys.Open;
      end
    else
      begin
        FSys.OpenNew;
        FMSys.OpenNew;
      end;

    FDatasysServer.TcpPort := FTcpPort;
    FDatasysServer.Start(FMSys);
  except
    on E : Exception do
      begin
        Log('!', 'Start error: %s', [E.Message]);
        raise;
      end;
  end;
  Log('#', 'Started');
end;

procedure TkvdsApplication.Stop;
begin
  Log('#', 'Stopping');
  FDatasysServer.Stop;
  FMSys.Close;
  FSys.Close;
  FreeAndNil(FMSys);
  FreeAndNil(FSys);
  Log('#', 'Stopped');
end;

procedure TkvdsApplication.Process;
begin
  Sleep(40);
end;

procedure TkvdsApplication.Log(const LogType: Char; const LogMsg: String;
          const LogLevel: Integer);
begin
  if Assigned(FOnLog) then
    FOnLog(LogType, LogMsg, LogLevel);
end;

procedure TkvdsApplication.Log(const LogType: Char; const LogMsg: String;
          const Args: array of const; const LogLevel: Integer);
begin
  Log(LogType, Format(LogMsg, Args), LogLevel);
end;

procedure TkvdsApplication.DatasysServerLog(LogType: Char; LogMsg: String;
          LogLevel: Integer);
begin
  Log(LogType, 'Datasys:%s', [LogMsg], LogLevel + 1);
end;

procedure TkvdsApplication.DatasysServerTextCommand(Server: TkvDatasysServer; const CmdText: String);
begin
  if FLogCommands then
    Log('#', 'Command: %s', [kvLogTextClean(CmdText)]);
end;

procedure TkvdsApplication.DatasysServerStopCommand(Server: TkvDatasysServer);
begin
  Lock;
  try
    FTerminated := True;
  finally
    Unlock;
  end;
end;

procedure TkvdsApplication.DatasysServerTextResponse(Server: TkvDatasysServer; const RespText: String);
begin
  if FLogCommands then
    Log('#', 'Response: %s', [kvLogTextClean(RespText)]);
end;



end.

