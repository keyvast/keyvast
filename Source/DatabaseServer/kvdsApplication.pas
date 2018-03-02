{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/19  0.01  Initial version, basic script execution }

// todo: binary protocol

unit kvdsApplication;

interface

uses
  SysUtils,
  SyncObjs,
  kvObjects,
  kvScriptParser,
  kvScriptSystem,
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

    FTerminated : Boolean;
    FParser     : TkvScriptParser;
    FSys        : TkvSystem;
    FMSys       : TkvScriptSystem;
    FTcpServer  : TIdTCPServer;

    procedure Lock;
    procedure Unlock;

    function  GetTerminated: Boolean;

    procedure Log(const LogType: Char; const LogMsg: String;
              const LogLevel: Integer = 0); overload;
    procedure Log(const LogType: Char; const LogMsg: String;
              const Args: array of const; const LogLevel: Integer = 0); overload;

    procedure TcpServerConnect(AContext: TIdContext);
    procedure TcpServerDisconnect(AContext: TIdContext);
    procedure TcpServerExecute(AContext: TIdContext);
    procedure TcpServerClientCommand(const AContext: TIdContext;
              const Session: TkvSession; const CmdTxt: String);

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
  FTcpPort := -1;
  FParser := TkvScriptParser.Create;
  FTcpServer := TIdTCPServer.Create(nil);
end;

destructor TkvdsApplication.Destroy;
begin
  FreeAndNil(FTcpServer);
  FreeAndNil(FMSys);
  FreeAndNil(FSys);
  FreeAndNil(FParser);
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
var
  P : Integer;
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
      FMSys.Open
    else
      FMSys.OpenNew;

    if FTcpPort < 0 then
      P := TCP_DefaultPort
    else
      P := FTcpPort;
    Log('#', 'Starting TCP server on port %d', [P]);

    FTcpServer.OnConnect := TcpServerConnect;
    FTcpServer.OnDisconnect := TcpServerDisconnect;
    FTcpServer.OnExecute := TcpServerExecute;
    FTcpServer.DefaultPort := P;
    FTcpServer.Active := True;
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
  FTcpServer.Active := False;
  FMSys.Close;
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


procedure TkvdsApplication.TcpServerConnect(AContext: TIdContext);
var
  Ses : TkvSession;
begin
  Ses := FMSys.AddSession;

  {$IFDEF IdContextUseDataObject}
  AContext.DataObject := Ses;
  {$ELSE}
  AContext.Data := Ses;
  {$ENDIF}

  Log('#', 'Session added: %s', [AContext.Connection.Socket.Binding.PeerIP]);
end;

procedure TkvdsApplication.TcpServerDisconnect(AContext: TIdContext);
var
  DaOb : TObject;
  Ses : TkvSession;
begin
  {$IFDEF IdContextUseDataObject}
  DaOb := AContext.DataObject;
  AContext.DataObject := nil;
  {$ELSE}
  DaOb := AContext.Data;
  AContext.Data := nil;
  {$ENDIF}
  if not Assigned(DaOb) then
    exit;
  Assert(DaOb is TkvSession);
  Ses := TkvSession(DaOb);

  Log('#', 'Session removed: %s', [AContext.Connection.Socket.Binding.PeerIP]);

  Ses.Close;
end;

procedure TkvdsApplication.TcpServerExecute(AContext: TIdContext);
var
  CmdS : String;
  DaOb : TObject;
  Ses : TkvSession;
begin
  CmdS := AContext.Connection.IOHandler.ReadLn(#13#10, -1, 65536, IndyTextEncoding_UTF8);

  {$IFDEF IdContextUseDataObject}
  DaOb := AContext.DataObject;
  {$ELSE}
  DaOb := AContext.Data;
  {$ENDIF}
  if not Assigned(DaOb) then
    exit;
  Assert(DaOb is TkvSession);
  Ses := TkvSession(DaOb);

  TcpServerClientCommand(AContext, Ses, CmdS);
end;

function kvCommandClean(const CmdTxt: String): String;
var
  R : Boolean;
  S : String;
  I : Integer;
begin
  R := False;
  S := CmdTxt;
  repeat
    I := S.IndexOf(#8);
    if I > 0 then
      S := S.Remove(I - 1, 2)
    else
    if I = 0 then
      S := S.Remove(0, 1)
    else
      R := True;
  until R;
  S := S.Trim;
  Result := S;
end;

procedure TkvdsApplication.TcpServerClientCommand(const AContext: TIdContext;
          const Session: TkvSession; const CmdTxt: String);
var
  CmdS : String;
  V : TObject;
  RespMsg : String;
  Resp : String;
  CloseCon : Boolean;
begin
  CloseCon := False;

  CmdS := kvCommandClean(CmdTxt);

  if FLogCommands then
    Log('#', 'Command: %s', [kvLogTextClean(CmdS)]);

  if CmdS = '' then
    RespMsg := '$nil'
  else
  if SameText(CmdS, 'exit') then
    begin
      RespMsg := '$bye';
      CloseCon := True;
    end
  else
  if SameText(CmdS, 'stop') then
    begin
      RespMsg := '$shuttingdown';
      CloseCon := True;
      Lock;
      FTerminated := True;
      Unlock;
    end
  else
    try
      V := Session.ExecScript(CmdS);
      if not Assigned(V) then
        RespMsg := '$nil'
      else
      if V is AkvValue then
        RespMsg := '$val:' + AkvValue(V).AsScript
      else
        RespMsg := '$unknownvaluetype';
    except
      on E : Exception do
        RespMsg := '$error:' + E.ClassName + ':' + E.Message;
    end;

  if FLogCommands then
    Log('#', 'Response: %s', [kvLogTextClean(RespMsg)]);

  Resp := RespMsg + WideCRLF;
  AContext.Connection.Socket.Write(Resp, IndyTextEncoding_UTF8);

  if CloseCon then
    AContext.Connection.Socket.CloseGracefully;
end;



end.

