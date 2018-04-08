{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/03/16  0.01  Initial development, binary protocol }
{ 2018/03/18  0.02  Iterator functions }

{$INCLUDE kvInclude.inc}

unit kvDatasysClient;

interface

uses
  SysUtils,
  SyncObjs,
  kvValues,
  IdGlobal,
  IdTCPClient;



type
  EkvDatasysClient = class(Exception);

  TkvDatabaseClientState = (
      dcsInit,
      dcsStarting,
      dcsStartFailed,
      dcsReady,
      dcsDisconnected,
      dcsStopping,
      dcsStopped
    );
  TkvDatasysClient = class
  private
    FHost : String;
    FPort : Integer;

    FLock      : TCriticalSection;
    FState     : TkvDatabaseClientState;
    FTcpClient : TIdTCPClient;

    procedure Lock;
    procedure Unlock;

    function  GetState: TkvDatabaseClientState;
    procedure SetState(const State: TkvDatabaseClientState);

    procedure TcpClientConnected(Sender: TObject);
    procedure TcpClientDisconnected(Sender: TObject);

  public
    constructor Create;
    destructor Destroy; override;

    property  Host: String read FHost write FHost;
    property  Port: Integer read FPort write FPort;

    procedure Start;
    procedure Stop;

    function  ExecTextCommand(const CommandText: String): String;
    function  ExecBinCommand(const RequestDict: TkvDictionaryValue): TkvDictionaryValue;

    function  ExecKqlText(const KqlText: String): AkvValue;

    procedure UseDataset(const DatabaseName, DatasetName: String);
    procedure Insert(const DatabaseName, DatasetName, Key: String; const Value: AkvValue);
    procedure Update(const DatabaseName, DatasetName, Key: String; const Value: AkvValue);
    procedure Append(const DatabaseName, DatasetName, Key: String; const Value: AkvValue);
    procedure Delete(const DatabaseName, DatasetName, Key: String);
    function  Select(const DatabaseName, DatasetName, Key: String): AkvValue;
    function  Exists(const DatabaseName, DatasetName, Key: String): Boolean;
    procedure MkPath(const DatabaseName, DatasetName, Key: String);
    function  ListOfKeys(const DatabaseName, DatasetName, KeyPath: String; const Recurse: Boolean): AkvValue;

    function  Iterate(const DatabaseName, DatasetName, Path: String;
              var Handle: Int64; var Key: String): Boolean;
    function  IterateNext(const Handle: Int64; var Key: String): Boolean;
    function  IterateGetValue(const Handle: Int64): AkvValue;
    procedure IterateFin(const Handle: Int64);
  end;



implementation



const
  WideCRLF = WideChar(#13) + WideChar(#10);



{ TkvDatasysClient }

constructor TkvDatasysClient.Create;
begin
  inherited Create;
  FState := dcsInit;
  FLock := TCriticalSection.Create;
  FTcpClient := TIdTCPClient.Create(nil);
  FTcpClient.OnConnected := TcpClientConnected;
  FTcpClient.OnDisconnected := TcpClientDisconnected;
end;

destructor TkvDatasysClient.Destroy;
begin
  FreeAndNil(FTcpClient);
  FreeAndNil(FLock);
  inherited Destroy;
end;

procedure TkvDatasysClient.Lock;
begin
  if Assigned(FLock) then
    FLock.Acquire;
end;

procedure TkvDatasysClient.Unlock;
begin
  if Assigned(FLock) then
    FLock.Release;
end;

function TkvDatasysClient.GetState: TkvDatabaseClientState;
begin
  Lock;
  try
    Result := FState;
  finally
    Unlock;
  end;
end;

procedure TkvDatasysClient.SetState(const State: TkvDatabaseClientState);
begin
  Lock;
  try
    FState := State;
  finally
    Unlock;
  end;
end;

procedure TkvDatasysClient.Start;
begin
  if FHost = '' then
    raise EkvDatasysClient.Create('Host not specified');
  if FPort <= 0 then
    raise EkvDatasysClient.Create('Port not specified');
  if FPort >= $FFFF then
    raise EkvDatasysClient.Create('Invalid port number');

  SetState(dcsStarting);
  try
    FTcpClient.Host := FHost;
    FTcpClient.Port := FPort;
    FTcpClient.Connect;
  except
    on E : Exception do
      begin
        SetState(dcsStartFailed);
        raise;
      end;
  end;

  SetState(dcsReady);
end;

procedure TkvDatasysClient.Stop;
var
  OldState : TkvDatabaseClientState;
begin
  OldState := GetState;
  Assert(OldState <> dcsInit);
  SetState(dcsStopping);
  if OldState = dcsReady then
    FTcpClient.Socket.CloseGracefully;
  SetState(dcsStopped);
end;

procedure TkvDatasysClient.TcpClientConnected(Sender: TObject);
begin
end;

procedure TkvDatasysClient.TcpClientDisconnected(Sender: TObject);
begin
  if GetState <> dcsStopping then
    SetState(dcsDisconnected);
end;

function TkvDatasysClient.ExecTextCommand(const CommandText: String): String;
var
  CmdSend : String;
  RespS : String;
begin
  if GetState <> dcsReady then
    raise EkvDatasysClient.Create('Not ready');

  CmdSend := CommandText + WideCRLF;
  FTcpClient.Socket.Write(CmdSend, IndyTextEncoding_UTF8);

  RespS := FTcpClient.IOHandler.ReadLn(#13#10, -1, 65536, IndyTextEncoding_UTF8);

  Result := RespS;
end;

procedure kvClientRequestDictToRequestBuf(const RequestDict: TkvDictionaryValue; var RequestBuf: TIdBytes);
var
  ReqDictSize : UInt32;
  ReqBufSize : UInt32;
begin
  Assert(Assigned(RequestDict));
  ReqDictSize := RequestDict.SerialSize;
  Assert(ReqDictSize > 0);
  ReqBufSize := ReqDictSize + 11;
  SetLength(RequestBuf, ReqBufSize);
  RequestBuf[0] := Ord('b');
  RequestBuf[1] := Ord('i');
  RequestBuf[2] := Ord('n');
  RequestBuf[3] := 13;
  RequestBuf[4] := 10;
  Move(ReqDictSize, RequestBuf[5], SizeOf(UInt32));
  RequestDict.GetSerialBuf(RequestBuf[9], ReqDictSize);
  RequestBuf[9 + ReqDictSize] := 13;
  RequestBuf[10 + ReqDictSize] := 10;
end;

procedure kvClientResponseBufToResponseDict(const ResponseBuf: TIdBytes; var ResponseDict: TkvDictionaryValue);
var
  RespSize : Integer;
begin
  RespSize := Length(ResponseBuf);
  ResponseDict := TkvDictionaryValue.Create;
  try
    Assert(RespSize > 0);
    ResponseDict.PutSerialBuf(ResponseBuf[0], RespSize);
  except
    on E : Exception do
      begin
        FreeAndNil(ResponseDict);
        raise EkvDatasysClient.CreateFmt('Invalid response encoding:%s', [E.Message]);
      end;
  end;
end;

function TkvDatasysClient.ExecBinCommand(const RequestDict: TkvDictionaryValue): TkvDictionaryValue;
var
  ReqBuf : TIdBytes;
  RespSize : UInt32;
  RespBuf : TIdBytes;
  RespDict : TkvDictionaryValue;
begin
  kvClientRequestDictToRequestBuf(RequestDict, ReqBuf);

  FTcpClient.Socket.Write(ReqBuf);

  RespSize := FTcpClient.IOHandler.ReadUInt32(False);
  if RespSize = 0 then
    raise EkvDatasysClient.Create('Invalid response');
  SetLength(RespBuf, RespSize);
  FTcpClient.IOHandler.ReadBytes(RespBuf, RespSize, False);

  kvClientResponseBufToResponseDict(RespBuf, RespDict);

  Result := RespDict;
end;

procedure CheckResponseError(const ResponseDict: TkvDictionaryValue);
begin
  if Assigned(ResponseDict) then
    if ResponseDict.Exists('error') then
      raise EkvDatasysClient.CreateFmt('Server:%s', [ResponseDict.GetValueAsString('error')]);
end;

function TkvDatasysClient.ExecKqlText(const KqlText: String): AkvValue;
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'exec_kql');
    Req.AddString('kql', KqlText);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
    if Assigned(Resp) then
      if Resp.Exists('value') then
        Result := AkvValue(Resp.ReleaseKey('value'))
      else
        Result := nil
    else
      raise EkvDatasysClient.Create('No response');
  finally
    Resp.Free;
  end;
end;

procedure TkvDatasysClient.UseDataset(const DatabaseName, DatasetName: String);
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'use');
    Req.AddString('db', DatabaseName);
    Req.AddString('ds', DatasetName);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
  finally
    Resp.Free;
  end;
end;

procedure TkvDatasysClient.Insert(const DatabaseName, DatasetName, Key: String;
          const Value: AkvValue);
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'key_command');
    Req.AddString('cmd', 'insert');
    Req.AddString('db', DatabaseName);
    Req.AddString('ds', DatasetName);
    Req.AddString('key', Key);
    Req.Add('val', Value.Duplicate);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
  finally
    Resp.Free;
  end;
end;

procedure TkvDatasysClient.Update(const DatabaseName, DatasetName, Key: String;
          const Value: AkvValue);
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'key_command');
    Req.AddString('cmd', 'update');
    Req.AddString('db', DatabaseName);
    Req.AddString('ds', DatasetName);
    Req.AddString('key', Key);
    Req.Add('val', Value.Duplicate);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
  finally
    Resp.Free;
  end;
end;

procedure TkvDatasysClient.Append(const DatabaseName, DatasetName, Key: String;
          const Value: AkvValue);
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'key_command');
    Req.AddString('cmd', 'append');
    Req.AddString('db', DatabaseName);
    Req.AddString('ds', DatasetName);
    Req.AddString('key', Key);
    Req.Add('val', Value.Duplicate);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
  finally
    Resp.Free;
  end;
end;

procedure TkvDatasysClient.Delete(const DatabaseName, DatasetName, Key: String);
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'key_command');
    Req.AddString('cmd', 'delete');
    Req.AddString('db', DatabaseName);
    Req.AddString('ds', DatasetName);
    Req.AddString('key', Key);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
  finally
    Resp.Free;
  end;
end;

function TkvDatasysClient.Select(const DatabaseName, DatasetName, Key: String): AkvValue;
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'key_command');
    Req.AddString('cmd', 'select');
    Req.AddString('db', DatabaseName);
    Req.AddString('ds', DatasetName);
    Req.AddString('key', Key);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
    if not Assigned(Resp) then
      raise EkvDatasysClient.Create('No response');
    if not Resp.Exists('val') then
      raise EkvDatasysClient.Create('No value');
    Result := AkvValue(Resp.ReleaseKey('val'));
  finally
    Resp.Free;
  end;
end;

function TkvDatasysClient.Exists(const DatabaseName, DatasetName, Key: String): Boolean;
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'key_command');
    Req.AddString('cmd', 'exists');
    Req.AddString('db', DatabaseName);
    Req.AddString('ds', DatasetName);
    Req.AddString('key', Key);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
    if not Assigned(Resp) then
      raise EkvDatasysClient.Create('No response');
    if not Resp.Exists('val') then
      raise EkvDatasysClient.Create('No value');
    Result := Resp.GetValueAsBoolean('val');
  finally
    Resp.Free;
  end;
end;

procedure TkvDatasysClient.MkPath(const DatabaseName, DatasetName, Key: String);
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'key_command');
    Req.AddString('cmd', 'mkpath');
    Req.AddString('db', DatabaseName);
    Req.AddString('ds', DatasetName);
    Req.AddString('key', Key);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
  finally
    Resp.Free;
  end;
end;

function TkvDatasysClient.ListOfKeys(const DatabaseName, DatasetName, KeyPath: String; const Recurse: Boolean): AkvValue;
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'key_command');
    Req.AddString('cmd', 'listofkeys');
    Req.AddString('db', DatabaseName);
    Req.AddString('ds', DatasetName);
    Req.AddString('key', KeyPath);
    Req.AddBoolean('recurse', Recurse);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
    if not Assigned(Resp) then
      raise EkvDatasysClient.Create('No response');
    if not Resp.Exists('val') then
      raise EkvDatasysClient.Create('No value');
    Result := AkvValue(Resp.ReleaseKey('val'));
  finally
    Resp.Free;
  end;
end;

function TkvDatasysClient.Iterate(const DatabaseName, DatasetName, Path: String;
         var Handle: Int64; var Key: String): Boolean;
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'iterate');
    Req.AddString('db', DatabaseName);
    Req.AddString('ds', DatasetName);
    Req.AddString('path', Path);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
    Result := Resp.GetValueAsBoolean('hasvalue');
    if Result then
      begin
        Handle := Resp.GetValueAsInteger('handle');
        Key := Resp.GetValueAsString('key');
      end
    else
      begin
        Handle := 0;
        Key := '';
      end;
  finally
    Resp.Free;
  end;
end;

function TkvDatasysClient.IterateNext(const Handle: Int64; var Key: String): Boolean;
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'iterate_next');
    Req.AddInteger('handle', Handle);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
    Result := Resp.GetValueAsBoolean('hasvalue');
    if Result then
      Key := Resp.GetValueAsString('key')
    else
      Key := '';
  finally
    Resp.Free;
  end;
end;

function TkvDatasysClient.IterateGetValue(const Handle: Int64): AkvValue;
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'iterate_getvalue');
    Req.AddInteger('handle', Handle);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
    Result := AkvValue(Resp.ReleaseKey('value'));
  finally
    Resp.Free;
  end;
end;

procedure TkvDatasysClient.IterateFin(const Handle: Int64);
var
  Req : TkvDictionaryValue;
  Resp : TkvDictionaryValue;
begin
  Req := TkvDictionaryValue.Create;
  try
    Req.AddString('request_type', 'iterate_fin');
    Req.AddInteger('handle', Handle);
    Resp := ExecBinCommand(Req);
  finally
    Req.Free;
  end;
  try
    CheckResponseError(Resp);
  finally
    Resp.Free;
  end;
end;



end.

