{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/03/16  0.01  Initial development, binary protocol }
{ 2018/03/18  0.02  Iterator functions }

{$INCLUDE kvInclude.inc}

unit kvDatasysServer;

interface

uses
  SysUtils,
  SyncObjs,
  kvValues,
  kvObjects,
  kvScriptParser,
  kvScriptSystem,
  IdGlobal,
  IdContext,
  IdIOHandler,
  IdTCPServer;



type
  EkvDatasysServer = class(Exception);

  TkvDatasysServer = class;

  TkvDatasysServerLogEvent = procedure (LogType: Char; LogMsg: String;
      LogLevel: Integer) of object;

  TkvDatasysServerEvent = procedure (Server: TkvDatasysServer) of object;

  TkvDatasysServerCommandEvent = procedure (Server: TkvDatasysServer; const CmdText: String) of object;

  TkvDatasysServerResponseEvent = procedure (Server: TkvDatasysServer; const RespText: String) of object;

  TkvDatasysServer = class
  private
    FTcpPort        : Integer;
    FOnLog          : TkvDatasysServerLogEvent;
    FOnTextCommand  : TkvDatasysServerCommandEvent;
    FOnTextResponse : TkvDatasysServerResponseEvent;
    FOnStopCommand  : TkvDatasysServerEvent;

    FTcpServer : TIdTCPServer;
    FSys       : TkvScriptSystem;

    procedure Log(const LogType: Char; const LogMsg: String;
              const LogLevel: Integer = 0); overload;
    procedure Log(const LogType: Char; const LogMsg: String;
              const Args: array of const; const LogLevel: Integer = 0); overload;

    procedure TcpServerConnect(AContext: TIdContext);
    procedure TcpServerDisconnect(AContext: TIdContext);
    procedure TcpServerExecute(AContext: TIdContext);

    procedure ClientTextCommand(const AContext: TIdContext;
              const Session: TkvScriptSession; const CmdTxt: String);
    procedure ClientBinCommand(const AContext: TIdContext;
              const Session: TkvScriptSession; const BinBuf; const BinBufSize: Integer);
    procedure ClientBinDictCommand(const AContext: TIdContext;
              const Session: TkvScriptSession; const RequestType: String;
              const RequestDict: TkvDictionaryValue);
    procedure ClientBinKeyCommand(const AContext: TIdContext;
              const Session: TkvScriptSession;
              const RequestDict: TkvDictionaryValue;
              const ResponseDict: TkvDictionaryValue);
    procedure ClientBinUseCommand(const AContext: TIdContext;
              const Session: TkvScriptSession;
              const RequestDict: TkvDictionaryValue;
              const ResponseDict: TkvDictionaryValue);
    procedure ClientBinIterateCommand(const AContext: TIdContext;
              const Session: TkvScriptSession;
              const RequestDict: TkvDictionaryValue;
              const ResponseDict: TkvDictionaryValue);
    procedure ClientBinIterateNextCommand(const AContext: TIdContext;
              const Session: TkvScriptSession;
              const RequestDict: TkvDictionaryValue;
              const ResponseDict: TkvDictionaryValue);
    procedure ClientBinIterateGetValueCommand(const AContext: TIdContext;
              const Session: TkvScriptSession;
              const RequestDict: TkvDictionaryValue;
              const ResponseDict: TkvDictionaryValue);
    procedure ClientBinIterateGetTimestampCommand(const AContext: TIdContext;
              const Session: TkvScriptSession;
              const RequestDict: TkvDictionaryValue;
              const ResponseDict: TkvDictionaryValue);
    procedure ClientBinIterateFinaliseCommand(const AContext: TIdContext;
              const Session: TkvScriptSession;
              const RequestDict: TkvDictionaryValue;
              const ResponseDict: TkvDictionaryValue);

  public
    constructor Create(const DefaultPort: Integer);
    destructor Destroy; override;

    property  TcpPort: Integer read FTcpPort write FTcpPort;
    property  OnLog: TkvDatasysServerLogEvent read FOnLog write FOnLog;
    property  OnTextCommand: TkvDatasysServerCommandEvent read FOnTextCommand write FOnTextCommand;
    property  OnTextResponse: TkvDatasysServerResponseEvent read FOnTextResponse write FOnTextResponse;
    property  OnStopCommand: TkvDatasysServerEvent read FOnStopCommand write FOnStopCommand;

    procedure Start(const Sys: TkvScriptSystem);
    procedure Stop;
  end;



implementation



const
  WideCRLF = WideChar(#13) + WideChar(#10);



constructor TkvDatasysServer.Create(const DefaultPort: Integer);
begin
  Assert(DefaultPort > 0);
  inherited Create;
  FTcpPort := DefaultPort;
  FTcpServer := TIdTCPServer.Create(nil);
end;

destructor TkvDatasysServer.Destroy;
begin
  FreeAndNil(FTcpServer);
  inherited Destroy;
end;

procedure TkvDatasysServer.Log(const LogType: Char; const LogMsg: String;
          const LogLevel: Integer);
begin
  if Assigned(FOnLog) then
    FOnLog(LogType, LogMsg, LogLevel);
end;

procedure TkvDatasysServer.Log(const LogType: Char; const LogMsg: String;
          const Args: array of const; const LogLevel: Integer);
begin
  Log(LogType, Format(LogMsg, Args), LogLevel);
end;

procedure TkvDatasysServer.Start(const Sys: TkvScriptSystem);
var
  P : Integer;
begin
  try
    Assert(Assigned(Sys));
    FSys := Sys;

    if (FTcpPort <= 0) or (FTcpPort >= $FFFF) then
      raise EkvDatasysServer.Create('Invalid TCP port');
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

procedure TkvDatasysServer.Stop;
begin
  Log('#', 'Stopping');
  FTcpServer.Active := False;
  Log('#', 'Stopped');
end;

{$IFDEF LINUX}
{$DEFINE IdContextUseDataObject}
{$ENDIF}

procedure TkvDatasysServer.TcpServerConnect(AContext: TIdContext);
var
  Ses : TkvScriptSession;
begin
  Ses := FSys.AddSession;

  {$IFDEF IdContextUseDataObject}
  AContext.DataObject := Ses;
  {$ELSE}
  AContext.Data := Ses;
  {$ENDIF}

  Log('#', 'Session added: %s', [AContext.Connection.Socket.Binding.PeerIP]);
end;

procedure TkvDatasysServer.TcpServerDisconnect(AContext: TIdContext);
var
  DaOb : TObject;
  Ses : TkvScriptSession;
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
  Assert(DaOb is TkvScriptSession);
  Ses := TkvScriptSession(DaOb);

  Log('#', 'Session removed: %s', [AContext.Connection.Socket.Binding.PeerIP]);

  Ses.Close;
end;

procedure TkvDatasysServer.TcpServerExecute(AContext: TIdContext);
var
  IOHandler : TIdIOHandler;
  CmdS : String;
  DaOb : TObject;
  Ses : TkvScriptSession;
  BinSize : UInt32;
  BinBuf : TIdBytes;
begin
  IOHandler := AContext.Connection.IOHandler;
  CmdS := IOHandler.ReadLn(#13#10, -1, 65536, IndyTextEncoding_UTF8);

  {$IFDEF IdContextUseDataObject}
  DaOb := AContext.DataObject;
  {$ELSE}
  DaOb := AContext.Data;
  {$ENDIF}
  if not Assigned(DaOb) then
    exit;
  Assert(DaOb is TkvScriptSession);
  Ses := TkvScriptSession(DaOb);

  if CmdS = 'bin' then
    begin
      BinSize := IOHandler.ReadUInt32(False);
      SetLength(BinBuf, BinSize);
      if BinSize > 0 then
        begin
          IOHandler.ReadBytes(BinBuf, BinSize, False);
          ClientBinCommand(AContext, Ses, BinBuf[0], BinSize);
        end;
      if IOHandler.ReadUInt16(False) <> $0A0D then
        AContext.Connection.Socket.Close;
    end
  else
    ClientTextCommand(AContext, Ses, CmdS);
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

procedure TkvDatasysServer.ClientTextCommand(const AContext: TIdContext;
          const Session: TkvScriptSession; const CmdTxt: String);
var
  CmdS : String;
  V : TObject;
  RespMsg : String;
  Resp : String;
  CloseCon : Boolean;
begin
  CloseCon := False;

  CmdS := kvCommandClean(CmdTxt);

  if Assigned(FOnTextCommand) then
    FOnTextCommand(self, CmdS);

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
      if Assigned(FOnStopCommand) then
        FOnStopCommand(self);
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

  if Assigned(FOnTextResponse) then
    FOnTextResponse(self, RespMsg);

  Resp := RespMsg + WideCRLF;
  AContext.Connection.Socket.Write(Resp, IndyTextEncoding_UTF8);

  if CloseCon then
    AContext.Connection.Socket.CloseGracefully;
end;

procedure TkvDatasysServer.ClientBinCommand(const AContext: TIdContext;
          const Session: TkvScriptSession; const BinBuf; const BinBufSize: Integer);
var
  ReqDict : TkvDictionaryValue;
  ReqType : String;
begin
  ReqDict := TkvDictionaryValue.Create;
  try
    try
      Assert(BinBufSize > 0);
      ReqDict.PutSerialBuf(BinBuf, BinBufSize);
      ReqType := ReqDict.GetValueAsString('request_type');
    except
      on E : Exception do
        begin
          Log('!', 'Invalid binary command encoding:%s', [E.Message]);
          AContext.Connection.Socket.Close;
          exit;
        end;
    end;
    ClientBinDictCommand(AContext, Session, ReqType, ReqDict);
  finally
    ReqDict.Free;
  end;
end;

procedure kvServerResponseDictToResponseBuf(const ResponseDict: TkvDictionaryValue;
          var ResponseBuf: TIdBytes);
var
  RespDictSize : UInt32;
  RespBufSize : Integer;
begin
  Assert(Assigned(ResponseDict));
  RespDictSize := ResponseDict.SerialSize;
  Assert(RespDictSize > 0);
  RespBufSize := RespDictSize + 4;
  SetLength(ResponseBuf, RespBufSize);
  Move(RespDictSize, ResponseBuf[0], SizeOf(UInt32));
  ResponseDict.GetSerialBuf(ResponseBuf[4], RespBufSize);
end;

procedure TkvDatasysServer.ClientBinDictCommand(const AContext: TIdContext;
          const Session: TkvScriptSession; const RequestType: String;
          const RequestDict: TkvDictionaryValue);
var
  ResponseDict : TkvDictionaryValue;
  RespType : String;
  CloseCon : Boolean;
  V : AkvValue;
  CmdS : String;
  ErrorS : String;
  RespBuf : TIdBytes;
begin
  CloseCon := False;
  ResponseDict := TkvDictionaryValue.Create;
  try
    try
      if RequestType = '' then
        RespType := 'nil'
      else
      if RequestType = 'exec_kql' then
        begin
          CmdS := RequestDict.GetValueAsString('kql');
          V := Session.ExecScript(CmdS);
          RespType := 'kql_result';
          if Assigned(V) then
            if V is AkvValue then
              ResponseDict.Add('value', AkvValue(V))
            else
              ErrorS := 'unknown_value_type';
        end
      else
      if RequestType = 'key_command' then
        begin
          ClientBinKeyCommand(AContext, Session, RequestDict, ResponseDict);
          RespType := 'key_command_response';
        end
      else
      if RequestType = 'iterate_next' then
        begin
          ClientBinIterateNextCommand(AContext, Session, RequestDict, ResponseDict);
          RespType := 'iterate_next_response';
        end
      else
      if RequestType = 'iterate_getvalue' then
        begin
          ClientBinIterateGetValueCommand(AContext, Session, RequestDict, ResponseDict);
          RespType := 'iterate_getvalue_response';
        end
      else
      if RequestType = 'iterate_gettimestamp' then
        begin
          ClientBinIterateGetTimestampCommand(AContext, Session, RequestDict, ResponseDict);
          RespType := 'iterate_gettimestamp_response';
        end
      else
      if RequestType = 'iterate' then
        begin
          ClientBinIterateCommand(AContext, Session, RequestDict, ResponseDict);
          RespType := 'iterate_response';
        end
      else
      if RequestType = 'iterate_fin' then
        begin
          ClientBinIterateFinaliseCommand(AContext, Session, RequestDict, ResponseDict);
          RespType := 'iterate_fin_response';
        end
      else
      if RequestType = 'use' then
        begin
          ClientBinUseCommand(AContext, Session, RequestDict, ResponseDict);
          RespType := 'use_response';
        end
      else
      if RequestType = 'exit' then
        begin
          RespType := 'bye';
          CloseCon := True;
        end
      else
      if RequestType = 'stop' then
        begin
          RespType := 'shuttingdown';
          CloseCon := True;
          if Assigned(FOnStopCommand) then
            FOnStopCommand(self);
        end
      else
        ErrorS := 'unknownrequesttype';
    except
      on E : Exception do
        ErrorS := E.ClassName + ':' + E.Message;
    end;

    if ErrorS <> '' then
      begin
        ResponseDict.AddString('response_type', 'error');
        ResponseDict.AddString('error', ErrorS);
      end
    else
      ResponseDict.AddString('response_type', RespType);

    kvServerResponseDictToResponseBuf(ResponseDict, RespBuf);

    AContext.Connection.Socket.Write(RespBuf);

    if CloseCon then
      AContext.Connection.Socket.CloseGracefully;
  finally
    ResponseDict.Free;
  end;
end;

procedure TkvDatasysServer.ClientBinKeyCommand(const AContext: TIdContext;
          const Session: TkvScriptSession; const RequestDict: TkvDictionaryValue;
          const ResponseDict: TkvDictionaryValue);
var
  KeyCmdS : String;
  DbS, DsS : String;
  KeyS : String;
  Val : AkvValue;
  ValSel : AkvValue;
begin
  KeyCmdS := RequestDict.GetValueAsString('cmd');
  DbS := RequestDict.GetValueAsString('db');
  DsS := RequestDict.GetValueAsString('ds');
  KeyS := RequestDict.GetValueAsString('key');
  if KeyCmdS = 'insert' then
    begin
      Val := RequestDict.GetValue('val');
      Session.AddRecord(DbS, DsS, KeyS, Val, Session.GetOptionPaths);
    end
  else
  if KeyCmdS = 'update' then
    begin
      Val := RequestDict.GetValue('val');
      Session.SetRecord(DbS, DsS, KeyS, Val, Session.GetOptionPaths);
    end
  else
  if KeyCmdS = 'delete' then
    Session.DeleteRecord(DbS, DsS, KeyS, Session.GetOptionPaths)
  else
  if KeyCmdS = 'select' then
    begin
      ValSel := Session.GetRecord(DbS, DsS, KeyS, Session.GetOptionPaths);
      ResponseDict.Add('val', ValSel);
    end
  else
  if KeyCmdS = 'append' then
    begin
      Val := RequestDict.GetValue('val');
      Session.AppendRecord(DbS, DsS, KeyS, Val, Session.GetOptionPaths);
    end
  else
  if KeyCmdS = 'exists' then
    ResponseDict.AddBoolean('val', Session.RecordExists(DbS, DsS, KeyS,
        Session.GetOptionPaths))
  else
  if KeyCmdS = 'mkpath' then
    Session.MakePath(DbS, DsS, KeyS)
  else
  if KeyCmdS = 'listofkeys' then
    begin
      Val := Session.ListOfKeys(DbS, DsS, KeyS,
          RequestDict.GetValueAsBoolean('recurse'),
          Session.GetOptionPaths);
      ResponseDict.Add('val', Val);
    end
  else
    raise EkvDatasysServer.Create('Unrecognised key command');
end;

procedure TkvDatasysServer.ClientBinUseCommand(const AContext: TIdContext;
          const Session: TkvScriptSession; const RequestDict: TkvDictionaryValue;
          const ResponseDict: TkvDictionaryValue);
var
  DbS, DsS : String;
begin
  DbS := RequestDict.GetValueAsString('db');
  DsS := RequestDict.GetValueAsString('ds');
  Session.UseDataset(DbS, DsS);
end;

const
  DatasysServer_IteratorMagic = $77123488;

type
  TkvDatasysServerDatasetIterator = record
    Magic : Word32;
    Iter  : TkvDatasetIterator;
  end;
  PkvDatasysServerDatasetIterator = ^TkvDatasysServerDatasetIterator;

procedure ValidateIterator(const Iter: PkvDatasysServerDatasetIterator);
begin
  if not Assigned(Iter) then
    raise EkvDatasysServer.Create('Invalid handle');
  if Iter^.Magic <> DatasysServer_IteratorMagic then
    raise EkvDatasysServer.Create('Invalid handle');
end;

procedure TkvDatasysServer.ClientBinIterateCommand(const AContext: TIdContext;
          const Session: TkvScriptSession; const RequestDict: TkvDictionaryValue;
          const ResponseDict: TkvDictionaryValue);
var
  DbS, DsS, PathS : String;
  Iter : PkvDatasysServerDatasetIterator;
  IterR : Boolean;
begin
  DbS := RequestDict.GetValueAsString('db');
  DsS := RequestDict.GetValueAsString('ds');
  PathS := RequestDict.GetValueAsString('path');
  New(Iter);
  try
    Iter.Magic := DatasysServer_IteratorMagic;
    IterR := Session.IterateRecords(DbS, DsS, PathS, Iter^.Iter);
  except
    Dispose(Iter);
    raise;
  end;
  ResponseDict.AddBoolean('hasvalue', IterR);
  if not IterR then
    Dispose(Iter)
  else
    begin
      ResponseDict.AddInteger('handle', Int64(Iter));
      ResponseDict.AddString('key', Session.IteratorGetKey(Iter^.Iter));
    end;
end;

procedure TkvDatasysServer.ClientBinIterateNextCommand(const AContext: TIdContext;
          const Session: TkvScriptSession; const RequestDict: TkvDictionaryValue;
          const ResponseDict: TkvDictionaryValue);
var
  HandleInt : Int64;
  Iter : PkvDatasysServerDatasetIterator;
  IterR : Boolean;
begin
  HandleInt := RequestDict.GetValueAsInteger('handle');
  Iter := Pointer(HandleInt);
  ValidateIterator(Iter);
  IterR := Session.IterateNextRecord(Iter^.Iter);
  ResponseDict.AddBoolean('hasvalue', IterR);
  if IterR then
    ResponseDict.AddString('key', Session.IteratorGetKey(Iter^.Iter));
end;

procedure TkvDatasysServer.ClientBinIterateGetValueCommand(const AContext: TIdContext;
          const Session: TkvScriptSession; const RequestDict: TkvDictionaryValue;
          const ResponseDict: TkvDictionaryValue);
var
  HandleInt : Int64;
  Iter : PkvDatasysServerDatasetIterator;
  ItVal : AkvValue;
begin
  HandleInt := RequestDict.GetValueAsInteger('handle');
  Iter := Pointer(HandleInt);
  ValidateIterator(Iter);
  ItVal := Session.IteratorGetValue(Iter^.Iter);
  ResponseDict.Add('value', ItVal);
end;

procedure TkvDatasysServer.ClientBinIterateGetTimestampCommand(const AContext: TIdContext;
          const Session: TkvScriptSession; const RequestDict: TkvDictionaryValue;
          const ResponseDict: TkvDictionaryValue);
var
  HandleInt : Int64;
  Iter : PkvDatasysServerDatasetIterator;
  ItVal : Int64;
begin
  HandleInt := RequestDict.GetValueAsInteger('handle');
  Iter := Pointer(HandleInt);
  ValidateIterator(Iter);
  ItVal := Session.IteratorGetTimestamp(Iter^.Iter);
  ResponseDict.AddInteger('value', ItVal);
end;

procedure TkvDatasysServer.ClientBinIterateFinaliseCommand(const AContext: TIdContext;
          const Session: TkvScriptSession; const RequestDict: TkvDictionaryValue;
          const ResponseDict: TkvDictionaryValue);
var
  HandleInt : Int64;
  Iter : PkvDatasysServerDatasetIterator;
begin
  HandleInt := RequestDict.GetValueAsInteger('handle');
  Iter := Pointer(HandleInt);
  ValidateIterator(Iter);
  Dispose(Iter);
end;



end.

