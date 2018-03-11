{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/10  0.01  Initial development }
{ 2018/02/28  0.02  Improve usage of selected database/dataset }
{ 2018/03/01  0.03  Persistent stored procedures and related SysInfo }

{$INCLUDE kvInclude.inc}

unit kvScriptSystem;

interface

uses
  SysUtils,
  SyncObjs,
  kvHashList,
  kvValues,
  kvObjects,
  kvScriptContext,
  kvScriptNodes,
  kvScriptParser;



type
  { TkvSession }

  EkvSession = class(EkvObject);

  TkvSession = class;
  TkvScriptDatabase = class;
  TkvScriptSystemScope = class;
  TkvScriptSystem = class;

  TkvSessionScope = class(AkvScriptScope)
  private
    FSession     : TkvSession;
    FParentScope : TkvScriptSystemScope;
    FIdentifiers : TkvStringHashList;

  public
    constructor Create(const Session: TkvSession;
                const ParentScope: TkvScriptSystemScope);
    destructor Destroy; override;

    function  GetIdentifier(const Identifier: String): TObject; override;
    procedure SetIdentifier(const Identifier: String; const Value: TObject); override;
  end;

  TkvSession = class(AkvScriptSession)
  private
    FSystem : TkvScriptSystem;

    FLocalScope             : TkvSessionScope;
    FContext                : TkvScriptContext;
    FSelectedDatabaseName   : String;
    FSelectedDatasetName    : String;
    FSelectedDatabase       : TkvDatabase;
    FSelectedScriptDatabase : TkvScriptDatabase;
    FSelectedDataset        : TkvDataset;
    FScriptParser           : TkvScriptParser;

    function  UsedDatabaseName(const DatabaseName: String): String;
    function  UsedDatasetName(const DatasetName: String): String;

    function  ParseScript(const Script: String): AkvScriptNode;

  public
    constructor Create(const System: TkvScriptSystem);
    destructor Destroy; override;

    property  ScriptContext: TkvScriptContext read FContext;

    procedure Close;

    function  AllocateSystemUniqueId: UInt64; override;

    function  CreateDatabase(const Name: String): TkvDatabase; override;
    procedure DropDatabase(const Name: String); override;
    function  ListOfDatabases: TkvKeyNameArray; override;

    function  AllocateDatabaseUniqueId(const DatabaseName: String): UInt64; override;

    function  CreateDataset(const DatabaseName, DatasetName: String): TkvDataset; override;
    procedure DropDataset(const DatabaseName, DatasetName: String); override;
    function  ListOfDatasets(const DatabaseName: String): TkvKeyNameArray; override;

    function  AllocateDatasetUniqueId(const DatabaseName, DatasetName: String): UInt64; override;

    procedure UseDatabase(const Name: String); override;
    procedure UseDataset(const DatabaseName, DatasetName: String); override;
    procedure UseNone; override;
    function  GetSelectedDatabaseName: String; override;
    function  GetSelectedDatasetName: String; override;

    procedure AddRecord(const DatabaseName, DatasetName, Key: String; const Value: AkvValue); override;
    procedure MakePath(const DatabaseName, DatasetName, KeyPath: String); override;
    function  GetRecord(const DatabaseName, DatasetName, Key: String): AkvValue; override;
    function  RecordExists(const DatabaseName, DatasetName, Key: String): Boolean; override;
    procedure DeleteRecord(const DatabaseName, DatasetName, Key: String); override;
    procedure SetRecord(const DatabaseName, DatasetName, Key: String; const Value: AkvValue); override;
    procedure AppendRecord(const DatabaseName, DatasetName, Key: String; const Value: AkvValue); override;

    function  IterateRecords(const DatabaseName, DatasetName: String;
              const Path: String; out Iterator: TkvDatasetIterator): Boolean; override;
    function  IterateNextRecord(var Iterator: TkvDatasetIterator): Boolean; override;
    function  IteratorGetKey(const Iterator: TkvDatasetIterator): String; override;
    function  IteratorGetValue(const Iterator: TkvDatasetIterator): AkvValue; override;

    function  ExecScript(const S: String): AkvValue; override;

    procedure CreateStoredProcedure(const DatabaseName, ProcedureName, Script: String); override;
    procedure DropStoredProcedure(const DatabaseName, ProcedureName: String); override;
  end;

  { TkvScriptStoredProcedure }

  TkvScriptStoredProcedure = class
  private
    FScript : String;
    FProcValue : TkvScriptProcedureValue;

  public
    constructor Create(const Script: String);
    destructor Destroy; override;
  end;


  { TkvScriptDatabase }

  TkvScriptDatabase = class
  private
    FStoredProcList : TkvStringHashList;

    function GetStoredProcByName(const Name: String): TkvScriptStoredProcedure;

  public
    constructor Create;
    destructor Destroy; override;

    procedure AddStoredProc(const Name: String; const Script: String);
    procedure RemoveStoredProc(const Name: String);
  end;



  { TkvScriptSystem }

  TkvScriptSystemScope = class
  private
    FIdentifiers : TkvStringHashList;

  public
    constructor Create;
    destructor Destroy; override;

    function  GetIdentifier(const Session: TkvSession; const Identifier: String): TObject;
  end;

  TkvScriptSystem = class
  private
    FSystem : TkvSystem;

    FScope        : TkvScriptSystemScope;
    FSessionList  : array of TkvSession;
    FSessionLock  : TCriticalSection;
    FExecLock     : TCriticalSection;
    FDatabaseList : TkvStringHashList;

    function  GetSessionIndex(const Session: TkvSession): Integer;
    procedure RemoveSessionByIndex(const Index: Integer);
    procedure RemoveSession(const Session: TkvSession);

    function  GetScriptDatabaseByName(const Name: String): TkvScriptDatabase;
    function  AddScriptDatabase(const Name: String): TkvScriptDatabase;
    function  GetOrAddScriptDatabase(const Name: String): TkvScriptDatabase;
    procedure RemoveScriptDatabase(const Name: String);

    procedure LoadSysInfo;

    procedure DropDatabaseStoredProcedures(const DatabaseName: String);

  protected
    procedure SessionClose(const Session: TkvSession);

    function  AllocateSystemUniqueId(const Session: TkvSession): UInt64;

    function  CreateDatabase(const Session: TkvSession; const Name: String): TkvDatabase;
    procedure DropDatabase(const Session: TkvSession; const Name: String);
    function  ListOfDatabases(const Session: TkvSession): TkvKeyNameArray;

    function  AllocateDatabaseUniqueId(const Session: TkvSession; const DatabaseName: String): UInt64;

    function  CreateDataset(const Session: TkvSession; const DatabaseName, DatasetName: String): TkvDataset;
    procedure DropDataset(const Session: TkvSession; const DatabaseName, DatasetName: String);
    function  ListOfDatasets(const Session: TkvSession; const DatabaseName: String): TkvKeyNameArray;

    function  AllocateDatasetUniqueId(const Session: TkvSession; const DatabaseName, DatasetName: String): UInt64;

    procedure UseDatabase(const Session: TkvSession; const Name: String;
              out Database: TkvDatabase; out ScriptDatabase: TkvScriptDatabase);
    procedure UseDataset(const Session: TkvSession; const DatabaseName, DatasetName: String;
              out Database: TkvDatabase; out ScriptDatabase: TkvScriptDatabase;
              out Dataset: TkvDataset);
    procedure UseNone(const Session: TkvSession);

    procedure AddRecord(const Session: TkvSession;
              const DatabaseName, DatasetName, Key: String;
              const Value: AkvValue); overload;
    procedure AddRecord(const Session: TkvSession; const Dataset: TkvDataset;
              const Key: String; const Value: AkvValue); overload;

    procedure MakePath(const Session: TkvSession;
              const DatabaseName, DatasetName, KeyPath: String); overload;
    procedure MakePath(const Session: TkvSession; const Dataset: TkvDataset;
              const KeyPath: String); overload;

    function  RecordExists(const Session: TkvSession;
              const DatabaseName, DatasetName, Key: String): Boolean; overload;
    function  RecordExists(const Session: TkvSession; const Dataset: TkvDataset;
              const Key: String): Boolean; overload;

    function  GetRecord(const Session: TkvSession;
              const DatabaseName, DatasetName, Key: String): AkvValue; overload;
    function  GetRecord(const Session: TkvSession; const Dataset: TkvDataset;
              const Key: String): AkvValue; overload;

    procedure DeleteRecord(const Session: TkvSession;
              const DatabaseName, DatasetName, Key: String); overload;
    procedure DeleteRecord(const Session: TkvSession; const Dataset: TkvDataset;
              const Key: String); overload;

    procedure SetRecord(const Session: TkvSession;
              const DatabaseName, DatasetName, Key: String;
              const Value: AkvValue); overload;
    procedure SetRecord(const Session: TkvSession; const Dataset: TkvDataset;
              const Key: String; const Value: AkvValue); overload;

    procedure AppendRecord(const Session: TkvSession;
              const DatabaseName, DatasetName, Key: String;
              const Value: AkvValue); overload;
    procedure AppendRecord(const Session: TkvSession; const Dataset: TkvDataset;
              const Key: String; const Value: AkvValue); overload;

    function  IterateRecords(const Session: TkvSession;
              const DatabaseName, DatasetName: String;
              const Path: String;
              out Iterator: TkvDatasetIterator): Boolean;
    function  IterateNextRecord(const Session: TkvSession; var Iterator: TkvDatasetIterator): Boolean;
    function  IteratorGetKey(const Session: TkvSession; const Iterator: TkvDatasetIterator): String;
    function  IteratorGetValue(const Session: TkvSession; const Iterator: TkvDatasetIterator): AkvValue;

    procedure CreateStoredProcedure(const Session: TkvSession;
              const DatabaseName, ProcedureName, Script: String);
    procedure DropStoredProcedure(const Session: TkvSession;
              const DatabaseName, ProcedureName: String);

  public
    constructor Create(const System: TkvSystem);
    destructor Destroy; override;

    procedure Open;
    procedure OpenNew;
    procedure Close;
    procedure Delete;

    function  GetSessionCount: Integer;
    procedure ClearSessions;
    function  AddSession: TkvSession;
  end;



implementation

uses
  kvScriptFunctions;



{ TkvSessionScope }

constructor TkvSessionScope.Create(const Session: TkvSession;
            const ParentScope: TkvScriptSystemScope);
begin
  Assert(Assigned(Session));
  Assert(Assigned(ParentScope));
  inherited Create;
  FSession := Session;
  FParentScope := ParentScope;
  FIdentifiers := TkvStringHashList.Create(False, False, True);
end;

destructor TkvSessionScope.Destroy;
begin
  FreeAndNil(FIdentifiers);
  inherited Destroy;
end;

function TkvSessionScope.GetIdentifier(const Identifier: String): TObject;
var
  F : Boolean;
  R : TObject;
begin
  F := FIdentifiers.GetValue(Identifier, R);
  if not F then
    R := FParentScope.GetIdentifier(FSession, Identifier);
  Result := R;
end;

procedure TkvSessionScope.SetIdentifier(const Identifier: String;
          const Value: TObject);
begin
  if FIdentifiers.KeyExists(Identifier) then
    FIdentifiers.SetValue(Identifier, Value)
  else
    FIdentifiers.Add(Identifier, Value)
end;



{ TkvSession }

constructor TkvSession.Create(const System: TkvScriptSystem);
begin
  inherited Create;
  FSystem := System;
  FLocalScope := TkvSessionScope.Create(self, System.FScope);
  FContext := TkvScriptContext.Create(FLocalScope, self);
end;

destructor TkvSession.Destroy;
begin
  FreeAndNil(FContext);
  FreeAndNil(FLocalScope);
  FreeAndNil(FScriptParser);
  inherited Destroy;
end;

procedure TkvSession.Close;
begin
  FSystem.SessionClose(self);
end;

function TkvSession.AllocateSystemUniqueId: UInt64;
begin
  Result := FSystem.AllocateSystemUniqueId(self);
end;

function TkvSession.CreateDatabase(const Name: String): TkvDatabase;
begin
  Result := FSystem.CreateDatabase(self, Name);
end;

procedure TkvSession.DropDatabase(const Name: String);
begin
  FSystem.DropDatabase(self, Name);
end;

function TkvSession.ListOfDatabases: TkvKeyNameArray;
begin
  Result := FSystem.ListOfDatabases(self);
end;

function TkvSession.AllocateDatabaseUniqueId(const DatabaseName: String): UInt64;
begin
  Result := FSystem.AllocateDatabaseUniqueId(self, DatabaseName);
end;

function TkvSession.CreateDataset(const DatabaseName, DatasetName: String): TkvDataset;
begin
  Result := FSystem.CreateDataset(self, UsedDatabaseName(DatabaseName), DatasetName);
end;

procedure TkvSession.DropDataset(const DatabaseName, DatasetName: String);
begin
  FSystem.DropDataset(self, UsedDatabaseName(DatabaseName), DatasetName);
end;

function TkvSession.ListOfDatasets(const DatabaseName: String): TkvKeyNameArray;
begin
  Result := FSystem.ListOfDatasets(self, DatabaseName);
end;

function TkvSession.AllocateDatasetUniqueId(const DatabaseName, DatasetName: String): UInt64;
begin
  Result := FSystem.AllocateDatasetUniqueId(self, DatabaseName, DatasetName);
end;

procedure TkvSession.UseDatabase(const Name: String);
begin
  FSystem.UseDatabase(self, Name, FSelectedDatabase, FSelectedScriptDatabase);
  FSelectedDataset := nil;
  FSelectedDatabaseName := Name;
  FSelectedDatasetName := '';
end;

procedure TkvSession.UseDataset(const DatabaseName, DatasetName: String);
begin
  FSystem.UseDataset(self, DatabaseName, DatasetName,
      FSelectedDatabase, FSelectedScriptDatabase, FSelectedDataset);
  FSelectedDatabaseName := DatabaseName;
  FSelectedDatasetName := DatasetName;
end;

procedure TkvSession.UseNone;
begin
  FSystem.UseNone(self);
  FSelectedDatabase := nil;
  FSelectedScriptDatabase := nil;
  FSelectedDataset := nil;
  FSelectedDatabaseName := '';
  FSelectedDatasetName := '';
end;

function TkvSession.GetSelectedDatabaseName: String;
begin
  Result := FSelectedDatabaseName;
end;

function TkvSession.GetSelectedDatasetName: String;
begin
  Result := FSelectedDatasetName;
end;

function TkvSession.UsedDatabaseName(const DatabaseName: String): String;
begin
  if DatabaseName = '' then
    Result := FSelectedDatabaseName
  else
    Result := DatabaseName;
end;

function TkvSession.UsedDatasetName(const DatasetName: String): String;
begin
  if DatasetName = '' then
    Result := FSelectedDatasetName
  else
    Result := DatasetName;
end;

procedure TkvSession.AddRecord(const DatabaseName, DatasetName, Key: String;
          const Value: AkvValue);
begin
  if (DatasetName = '') and Assigned(FSelectedDataset) then
    FSystem.AddRecord(self, FSelectedDataset, Key, Value)
  else
    FSystem.AddRecord(self,
        UsedDatabaseName(DatabaseName),
        UsedDatasetName(DatasetName),
        Key, Value);
end;

procedure TkvSession.MakePath(const DatabaseName, DatasetName, KeyPath: String);
begin
  if (DatasetName = '') and Assigned(FSelectedDataset) then
    FSystem.MakePath(self, FSelectedDataset, KeyPath)
  else
    FSystem.MakePath(self,
        UsedDatabaseName(DatabaseName),
        UsedDatasetName(DatasetName),
        KeyPath);
end;

function TkvSession.GetRecord(const DatabaseName, DatasetName,
         Key: String): AkvValue;
begin
  if (DatasetName = '') and Assigned(FSelectedDataset) then
    Result := FSystem.GetRecord(self, FSelectedDataset, Key)
  else
    Result := FSystem.GetRecord(self,
        UsedDatabaseName(DatabaseName),
        UsedDatasetName(DatasetName),
        Key);
end;

function TkvSession.RecordExists(const DatabaseName, DatasetName, Key: String): Boolean;
begin
  if (DatasetName = '') and Assigned(FSelectedDataset) then
    Result := FSystem.RecordExists(self, FSelectedDataset, Key)
  else
    Result := FSystem.RecordExists(self,
        UsedDatabaseName(DatabaseName),
        UsedDatasetName(DatasetName),
        Key);
end;

procedure TkvSession.DeleteRecord(const DatabaseName, DatasetName, Key: String);
begin
  if (DatasetName = '') and Assigned(FSelectedDataset) then
    FSystem.DeleteRecord(self, FSelectedDataset, Key)
  else
    FSystem.DeleteRecord(self,
        UsedDatabaseName(DatabaseName),
        UsedDatasetName(DatasetName),
        Key);
end;

procedure TkvSession.SetRecord(const DatabaseName, DatasetName, Key: String;
          const Value: AkvValue);
begin
  if (DatasetName = '') and Assigned(FSelectedDataset) then
    FSystem.SetRecord(self, FSelectedDataset, Key, Value)
  else
    FSystem.SetRecord(self,
        UsedDatabaseName(DatabaseName),
        UsedDatasetName(DatasetName),
        Key, Value);
end;

procedure TkvSession.AppendRecord(const DatabaseName, DatasetName, Key: String; const Value: AkvValue);
begin
  if (DatasetName = '') and Assigned(FSelectedDataset) then
    FSystem.AppendRecord(self, FSelectedDataset, Key, Value)
  else
    FSystem.AppendRecord(self,
        UsedDatabaseName(DatabaseName),
        UsedDatasetName(DatasetName),
        Key, Value);
end;

function TkvSession.IterateRecords(const DatabaseName, DatasetName: String;
         const Path: String; out Iterator: TkvDatasetIterator): Boolean;
begin
  Result := FSystem.IterateRecords(self, DatabaseName, DatasetName, Path, Iterator);
end;

function TkvSession.IterateNextRecord(var Iterator: TkvDatasetIterator): Boolean;
begin
  Result := FSystem.IterateNextRecord(self, Iterator);
end;

function TkvSession.IteratorGetKey(const Iterator: TkvDatasetIterator): String;
begin
  Result := FSystem.IteratorGetKey(self, Iterator);
end;

function TkvSession.IteratorGetValue(const Iterator: TkvDatasetIterator): AkvValue;
begin
  Result := FSystem.IteratorGetValue(self, Iterator);
end;

function TkvSession.ParseScript(const Script: String): AkvScriptNode;
begin
  if not Assigned(FScriptParser) then
    FScriptParser := TkvScriptParser.Create;
  Result := FScriptParser.Parse(Script);
end;

function TkvSession.ExecScript(const S: String): AkvValue;
var
  N : AkvScriptNode;
  V : AkvValue;
begin
  N := ParseScript(S);
  try
    if N is AkvScriptStatement then
      V := AkvScriptStatement(N).Execute(FContext)
    else
    if N is AkvScriptExpression then
      V := AkvScriptExpression(N).Evaluate(FContext)
    else
      raise EkvSession.Create('Not an executable script');
  finally
    N.Free;
  end;
  Result := V;
end;

procedure TkvSession.CreateStoredProcedure(const DatabaseName, ProcedureName, Script: String);
begin
  FSystem.CreateStoredProcedure(self, DatabaseName, ProcedureName, Script);
end;

procedure TkvSession.DropStoredProcedure(const DatabaseName, ProcedureName: String);
begin
  FSystem.DropStoredProcedure(self, DatabaseName, ProcedureName);
end;



{ TkvScriptSessionSystemScope }

constructor TkvScriptSystemScope.Create;
begin
  inherited Create;
  FIdentifiers := TkvStringHashList.Create(False, False, True);
  FIdentifiers.Add('LEN', TkvScriptLengthBuiltInFunction.Create);
  FIdentifiers.Add('INTEGER', TkvScriptIntegerCastBuiltInFunction.Create);
  FIdentifiers.Add('FLOAT', TkvScriptFloatCastBuiltInFunction.Create);
  FIdentifiers.Add('STRING', TkvScriptStringCastBuiltInFunction.Create);
  FIdentifiers.Add('DATETIME', TkvScriptDateTimeCastBuiltInFunction.Create);
  FIdentifiers.Add('BYTE', TkvScriptByteCastBuiltInFunction.Create);
  FIdentifiers.Add('BINARY', TkvScriptBinaryCastBuiltInFunction.Create);
  FIdentifiers.Add('CHAR', TkvScriptCharCastBuiltInFunction.Create);
  FIdentifiers.Add('REPLACE', TkvScriptReplaceBuiltInFunction.Create);
  FIdentifiers.Add('GETDATE', TkvScriptGetDateBuiltInFunction.Create);
  FIdentifiers.Add('ISNULL', TkvScriptIsNullBuiltInFunction.Create);
  FIdentifiers.Add('LOWER', TkvScriptLowerBuiltInFunction.Create);
  FIdentifiers.Add('UPPER', TkvScriptUpperBuiltInFunction.Create);
  FIdentifiers.Add('TRIM', TkvScriptTrimBuiltInFunction.Create);
  FIdentifiers.Add('ROUND', TkvScriptRoundBuiltInFunction.Create);
  FIdentifiers.Add('SUBSTRING', TkvScriptSubstringBuiltInFunction.Create);
  FIdentifiers.Add('INDEXOF', TkvScriptIndexOfBuiltInFunction.Create);
  FIdentifiers.Add('LEFT', TkvScriptLeftBuiltInFunction.Create);
  FIdentifiers.Add('RIGHT', TkvScriptRightBuiltInFunction.Create);
  FIdentifiers.Add('SETOF', TkvScriptSetOfBuiltInFunction.Create);
  FIdentifiers.Add('DATE', TkvScriptDateBuiltInFunction.Create);
  FIdentifiers.Add('TIME', TkvScriptTimeBuiltInFunction.Create);
  FIdentifiers.Add('DECIMAL', TkvScriptDecimalCastBuiltInFunction.Create);
end;

destructor TkvScriptSystemScope.Destroy;
begin
  FreeAndNil(FIdentifiers);
  inherited Destroy;
end;

function TkvScriptSystemScope.GetIdentifier(const Session: TkvSession;
         const Identifier: String): TObject;
var
  F : Boolean;
  R : TObject;
  SP : TkvScriptStoredProcedure;
  N : AkvScriptNode;
begin
  F := FIdentifiers.GetValue(Identifier, R);
  if not F then
    begin
      if Assigned(Session.FSelectedScriptDatabase) then
        begin
          SP := Session.FSelectedScriptDatabase.GetStoredProcByName(Identifier);
          if Assigned(SP) then
            begin
              if not Assigned(SP.FProcValue) then
                begin
                  N := Session.ParseScript(SP.FScript);
                  try
                    if not (N is TkvScriptCreateProcedureStatement) then
                      raise EkvScriptScope.CreateFmt('Invalid stored procedure: %s', [Identifier]);
                    SP.FProcValue := TkvScriptCreateProcedureStatement(N).GetScriptProcedureValue;
                  finally
                    N.Free;
                  end;
                end;
              R := SP.FProcValue;
              F := True;
            end;
        end;
      if not F then
        raise EkvScriptScope.CreateFmt('Identifier not defined: %s', [Identifier]);
    end;
  Result := R;
end;



{ TkvScriptStoredProcedure }

constructor TkvScriptStoredProcedure.Create(const Script: String);
begin
  inherited Create;
  FScript := Script;
end;

destructor TkvScriptStoredProcedure.Destroy;
begin
  FreeAndNil(FProcValue);
  inherited Destroy;
end;



{ TkvScriptDatabase }

constructor TkvScriptDatabase.Create;
begin
  inherited Create;
  FStoredProcList := TkvStringHashList.Create(False, False, True);
end;

destructor TkvScriptDatabase.Destroy;
begin
  FreeAndNil(FStoredProcList);
  inherited Destroy;
end;

function TkvScriptDatabase.GetStoredProcByName(const Name: String): TkvScriptStoredProcedure;
var
  V : TObject;
begin
  if FStoredProcList.GetValue(Name, V) then
    Result := TkvScriptStoredProcedure(V)
  else
    Result := nil;
end;

procedure TkvScriptDatabase.AddStoredProc(const Name, Script: String);
var
  Proc : TkvScriptStoredProcedure;
begin
  Proc := TkvScriptStoredProcedure.Create(Script);
  FStoredProcList.Add(Name, Proc);
end;

procedure TkvScriptDatabase.RemoveStoredProc(const Name: String);
begin
  FStoredProcList.DeleteKey(Name);
end;



{ TkvScriptSessionSystem }

constructor TkvScriptSystem.Create(const System: TkvSystem);
begin
  Assert(Assigned(System));
  inherited Create;
  FSystem := System;
  FSessionLock := TCriticalSection.Create;
  FExecLock := TCriticalSection.Create;
  FDatabaseList := TkvStringHashList.Create(False, False, True);
  FScope := TkvScriptSystemScope.Create;
end;

destructor TkvScriptSystem.Destroy;
var
  I : Integer;
begin
  for I := Length(FSessionList) - 1 downto 0 do
    FreeAndNil(FSessionList[I]);
  FreeAndNil(FScope);
  FreeAndNil(FDatabaseList);
  FreeAndNil(FExecLock);
  FreeAndNil(FSessionLock);
  inherited Destroy;
end;

procedure TkvScriptSystem.Open;
begin
  FSystem.Open;
  LoadSysInfo;
end;

procedure TkvScriptSystem.OpenNew;
begin
  FSystem.OpenNew;
end;

procedure TkvScriptSystem.Close;
begin
  FSystem.Close;
  FDatabaseList.Clear;
end;

procedure TkvScriptSystem.Delete;
begin
  FSystem.Delete;
end;

function TkvScriptSystem.GetSessionCount: Integer;
begin
  FSessionLock.Acquire;
  try
    Result := Length(FSessionList);
  finally
    FSessionLock.Release;
  end;
end;

procedure TkvScriptSystem.ClearSessions;
var
  I : Integer;
begin
  FSessionLock.Acquire;
  try
    for I := Length(FSessionList) - 1 downto 0 do
     FreeAndNil(FSessionList[I]);
    SetLength(FSessionList, 0);
  finally
    FSessionLock.Release;
  end;
end;

function TkvScriptSystem.AddSession: TkvSession;
var
  S : TkvSession;
  L : Integer;
begin
  S := TkvSession.Create(self);
  FSessionLock.Acquire;
  try
    L := Length(FSessionList);
    SetLength(FSessionList, L + 1);
    FSessionList[L] := S;
  finally
    FSessionLock.Release;
  end;
  Result := S;
end;

function TkvScriptSystem.GetSessionIndex(const Session: TkvSession): Integer;
var
  I : Integer;
begin
  Assert(Assigned(Session));

  for I := 0 to Length(FSessionList) - 1 do
    if FSessionList[I] = Session then
      begin
        Result := I;
        exit;
      end;
  Result := -1;
end;

procedure TkvScriptSystem.RemoveSessionByIndex(const Index: Integer);
var
  I, L : Integer;
  Ses : TkvSession;
begin
  Assert(Index >= 0);
  Assert(Index < Length(FSessionList));

  Ses := FSessionList[Index];
  L := Length(FSessionList);
  for I := Index to L - 2 do
    FSessionList[I] := FSessionList[I + 1];
  SetLength(FSessionList, L - 1);
  Ses.Free;
end;

procedure TkvScriptSystem.RemoveSession(const Session: TkvSession);
var
  I : Integer;
begin
  FSessionLock.Acquire;
  try
    I := GetSessionIndex(Session);
    if I < 0 then
      raise EkvObject.Create('Session not found');
    RemoveSessionByIndex(I);
  finally
    FSessionLock.Release;
  end;
end;

function TkvScriptSystem.GetScriptDatabaseByName(const Name: String): TkvScriptDatabase;
var
  V : TObject;
begin
  if FDatabaseList.GetValue(Name, V) then
    Result := TkvScriptDatabase(V)
  else
    Result := nil;
end;

function TkvScriptSystem.AddScriptDatabase(const Name: String): TkvScriptDatabase;
begin
  Result := TkvScriptDatabase.Create;
  FDatabaseList.Add(Name, Result);
end;

function TkvScriptSystem.GetOrAddScriptDatabase(const Name: String): TkvScriptDatabase;
var
  Db : TkvScriptDatabase;
begin
  Db := GetScriptDatabaseByName(Name);
  if not Assigned(Db) then
    Db := AddScriptDatabase(Name);
  Result := Db;
end;

procedure TkvScriptSystem.RemoveScriptDatabase(const Name: String);
begin
  FDatabaseList.DeleteKey(Name);
end;

procedure TkvScriptSystem.LoadSysInfo;
var
  SI : TkvDataset;
  It : TkvDatasetIterator;
  Key : String;
  KeyP : TArray<String>;
  TypeS : String;
  Val : AkvValue;
  ValS : String;
  DbN : String;
  Db : TkvScriptDatabase;
  SpN : String;
begin
  SI := FSystem.SysInfoDataset;
  if SI.IterateRecords('', It) then
    repeat
      Key := SI.IteratorGetKey(It);
      KeyP := Key.Split([':'], 3);
      if Length(KeyP) >= 2 then
        begin
          TypeS := KeyP[0];
          DbN := KeyP[1];
          if TypeS = 'sp' then
            begin
              if Length(KeyP) >= 3 then
                begin
                  Db := GetOrAddScriptDatabase(DbN);
                  Val := SI.IteratorGetValue(It);
                  ValS := Val.AsString;
                  SpN := KeyP[2];
                  Db.AddStoredProc(SpN, ValS);
                end;
            end
          else
          if TypeS = 'db' then
            GetOrAddScriptDatabase(DbN)
        end;
    until not SI.IterateNextRecord(It);
end;

procedure TkvScriptSystem.SessionClose(const Session: TkvSession);
begin
  RemoveSession(Session);
end;

function TkvScriptSystem.AllocateSystemUniqueId(const Session: TkvSession): UInt64;
begin
  FExecLock.Acquire;
  try
    Result := FSystem.AllocateSystemUniqueId;
  finally
    FExecLock.Release;
  end;
end;

function TkvScriptSystem.CreateDatabase(const Session: TkvSession; const Name: String): TkvDatabase;
begin
  FExecLock.Acquire;
  try
    Result := FSystem.CreateDatabase(Name);
    FSystem.SysInfoDataset.AddRecordNull('db:' + Name);
    AddScriptDatabase(Name);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.DropDatabaseStoredProcedures(const DatabaseName: String);
var
  Db : TkvScriptDatabase;
  It : TkvStringHashListIterator;
  Im : PkvStringHashListItem;
begin
  Db := GetScriptDatabaseByName(DatabaseName);
  Assert(Assigned(Db));
  Im := Db.FStoredProcList.IterateFirst(It);
  while Assigned(Im) do
    begin
      FSystem.SysInfoDataset.DeleteRecord('sp:' + DatabaseName + ':' +Im^.Key);
      Im := Db.FStoredProcList.IterateNext(It);
    end;
end;

procedure TkvScriptSystem.DropDatabase(const Session: TkvSession; const Name: String);
begin
  FExecLock.Acquire;
  try
    FSystem.DropDatabase(Name);
    FSystem.SysInfoDataset.DeleteRecord('db:' + Name);
    DropDatabaseStoredProcedures(Name);
    RemoveScriptDatabase(Name);
  finally
    FExecLock.Release;
  end;
end;

function TkvScriptSystem.ListOfDatabases(const Session: TkvSession): TkvKeyNameArray;
var
  L, I : Integer;
  R : TkvKeyNameArray;
begin
  FExecLock.Acquire;
  try
    L := FSystem.GetDatabaseCount;
    SetLength(R, L);
    for I := 0 to L - 1 do
      R[I] := FSystem.GetDatabaseByIndex(I).Name;
  finally
    FExecLock.Release;
  end;
  Result := R;
end;

function TkvScriptSystem.AllocateDatabaseUniqueId(const Session: TkvSession;
         const DatabaseName: String): UInt64;
begin
  FExecLock.Acquire;
  try
    Result := FSystem.AllocateDatabaseUniqueId(DatabaseName);
  finally
    FExecLock.Release;
  end;
end;

function TkvScriptSystem.CreateDataset(const Session: TkvSession;
         const DatabaseName, DatasetName: String): TkvDataset;
begin
  FExecLock.Acquire;
  try
    Result := FSystem.CreateDataset(DatabaseName, DatasetName);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.DropDataset(const Session: TkvSession;
          const DatabaseName, DatasetName: String);
begin
  FExecLock.Acquire;
  try
    FSystem.DropDataset(DatabaseName, DatasetName);
  finally
    FExecLock.Release;
  end;
end;

function TkvScriptSystem.ListOfDatasets(const Session: TkvSession;
         const DatabaseName: String): TkvKeyNameArray;
var
  Db : TkvDatabase;
  L, I : Integer;
  R : TkvKeyNameArray;
begin
  FExecLock.Acquire;
  try
    Db := FSystem.RequireDatabaseByName(DatabaseName);
    L := Db.GetDatasetCount;
    SetLength(R, L);
    for I := 0 to L - 1 do
      R[I] := Db.GetDataset(I).Name;
  finally
    FExecLock.Release;
  end;
  Result := R;
end;

function TkvScriptSystem.AllocateDatasetUniqueId(const Session: TkvSession;
         const DatabaseName, DatasetName: String): UInt64;
begin
  FExecLock.Acquire;
  try
    Result := FSystem.AllocateDatasetUniqueId(DatabaseName, DatasetName);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.UseDatabase(const Session: TkvSession;
          const Name: String;
          out Database: TkvDatabase; out ScriptDatabase: TkvScriptDatabase);
begin
  FExecLock.Acquire;
  try
    if not FSystem.DatabaseExists(Name) then
      raise EkvSession.CreateFmt('Database does not exist: %s', [Name]);
    Database := FSystem.RequireDatabaseByName(Name);
    ScriptDatabase := GetScriptDatabaseByName(Name);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.UseDataset(
          const Session: TkvSession;
          const DatabaseName, DatasetName: String;
          out Database: TkvDatabase; out ScriptDatabase: TkvScriptDatabase;
          out Dataset: TkvDataset);
begin
  FExecLock.Acquire;
  try
    if not FSystem.DatabaseExists(DatabaseName) then
      raise EkvSession.CreateFmt('Database does not exist: %s', [DatabaseName]);
    if not FSystem.DatasetExists(DatabaseName, DatasetName) then
      raise EkvSession.CreateFmt('Dataset does not exist: %s', [DatasetName]);
    Database := FSystem.RequireDatabaseByName(DatabaseName);
    ScriptDatabase := GetScriptDatabaseByName(DatabaseName);
    Dataset := Database.RequireDatasetByName(DatasetName);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.UseNone(const Session: TkvSession);
begin
  Assert(Assigned(Session));
end;

procedure TkvScriptSystem.AddRecord(const Session: TkvSession;
          const DatabaseName, DatasetName, Key: String; const Value: AkvValue);
begin
  FExecLock.Acquire;
  try
    FSystem.AddRecord(DatabaseName, DatasetName, Key, Value);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.AddRecord(const Session: TkvSession;
          const Dataset: TkvDataset; const Key: String; const Value: AkvValue);
begin
  FExecLock.Acquire;
  try
    FSystem.AddRecord(Dataset, Key, Value);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.MakePath(const Session: TkvSession; const DatabaseName, DatasetName, KeyPath: String);
begin
  FExecLock.Acquire;
  try
    FSystem.MakePath(DatabaseName, DatasetName, KeyPath);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.MakePath(const Session: TkvSession; const Dataset: TkvDataset;
          const KeyPath: String);
begin
  FExecLock.Acquire;
  try
    FSystem.MakePath(Dataset, KeyPath);
  finally
    FExecLock.Release;
  end;
end;

function TkvScriptSystem.RecordExists(const Session: TkvSession; const DatabaseName, DatasetName, Key: String): Boolean;
begin
  FExecLock.Acquire;
  try
    Result := FSystem.RecordExists(DatabaseName, DatasetName, Key);
  finally
    FExecLock.Release;
  end;
end;

function TkvScriptSystem.RecordExists(const Session: TkvSession;
         const Dataset: TkvDataset; const Key: String): Boolean;
begin
  FExecLock.Acquire;
  try
    Result := FSystem.RecordExists(Dataset, Key);
  finally
    FExecLock.Release;
  end;
end;

function TkvScriptSystem.GetRecord(const Session: TkvSession; const DatabaseName, DatasetName, Key: String): AkvValue;
begin
  FExecLock.Acquire;
  try
    Result := FSystem.GetRecord(DatabaseName, DatasetName, Key);
  finally
    FExecLock.Release;
  end;
end;

function TkvScriptSystem.GetRecord(const Session: TkvSession;
         const Dataset: TkvDataset; const Key: String): AkvValue;
begin
  FExecLock.Acquire;
  try
    Result := FSystem.GetRecord(Dataset, Key);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.DeleteRecord(const Session: TkvSession; const DatabaseName, DatasetName, Key: String);
begin
  FExecLock.Acquire;
  try
    FSystem.DeleteRecord(DatabaseName, DatasetName, Key);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.DeleteRecord(const Session: TkvSession;
          const Dataset: TkvDataset;
          const Key: String);
begin
  FExecLock.Acquire;
  try
    FSystem.DeleteRecord(Dataset, Key);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.SetRecord(const Session: TkvSession;
          const DatabaseName, DatasetName, Key: String; const Value: AkvValue);
begin
  FExecLock.Acquire;
  try
    FSystem.SetRecord(DatabaseName, DatasetName, Key, Value);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.SetRecord(const Session: TkvSession;
          const Dataset: TkvDataset;
          const Key: String; const Value: AkvValue);
begin
  FExecLock.Acquire;
  try
    FSystem.SetRecord(Dataset, Key, Value);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.AppendRecord(const Session: TkvSession;
          const DatabaseName, DatasetName, Key: String;
          const Value: AkvValue);
begin
  FExecLock.Acquire;
  try
    FSystem.AppendRecord(DatabaseName, DatasetName, Key, Value);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.AppendRecord(const Session: TkvSession; const Dataset: TkvDataset;
          const Key: String; const Value: AkvValue);
begin
  FExecLock.Acquire;
  try
    FSystem.AppendRecord(Dataset, Key, Value);
  finally
    FExecLock.Release;
  end;
end;

function TkvScriptSystem.IterateRecords(const Session: TkvSession;
         const DatabaseName, DatasetName: String;
         const Path: String;
         out Iterator: TkvDatasetIterator): Boolean;
begin
  FExecLock.Acquire;
  try
    Result := FSystem.IterateRecords(DatabaseName, DatasetName, Path, Iterator);
  finally
    FExecLock.Release;
  end;
end;

function TkvScriptSystem.IterateNextRecord(const Session: TkvSession;
         var Iterator: TkvDatasetIterator): Boolean;
begin
  FExecLock.Acquire;
  try
    Result := FSystem.IterateNextRecord(Iterator);
  finally
    FExecLock.Release;
  end;
end;

function TkvScriptSystem.IteratorGetKey(const Session: TkvSession;
         const Iterator: TkvDatasetIterator): String;
begin
  FExecLock.Acquire;
  try
    Result := FSystem.IteratorGetKey(Iterator);
  finally
    FExecLock.Release;
  end;
end;

function TkvScriptSystem.IteratorGetValue(const Session: TkvSession;
         const Iterator: TkvDatasetIterator): AkvValue;
begin
  FExecLock.Acquire;
  try
    Result := FSystem.IteratorGetValue(Iterator);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.CreateStoredProcedure(const Session: TkvSession;
          const DatabaseName, ProcedureName, Script: String);
var
  Db : TkvScriptDatabase;
begin
  Assert(DatabaseName <> '');
  FExecLock.Acquire;
  try
    Db := GetOrAddScriptDatabase(DatabaseName);
    Db.AddStoredProc(ProcedureName, Script);

    FSystem.SysInfoDataset.AddRecordString(
        'sp:' + DatabaseName + ':' + ProcedureName,
        Script);
  finally
    FExecLock.Release;
  end;
end;

procedure TkvScriptSystem.DropStoredProcedure(const Session: TkvSession;
          const DatabaseName, ProcedureName: String);
var
  Db : TkvScriptDatabase;
begin
  FExecLock.Acquire;
  try
    Db := GetScriptDatabaseByName(DatabaseName);
    if not Assigned(Db) then
      raise EkvSession.CreateFmt('Database not found: %s', [DatabaseName]);
    Db.RemoveStoredProc(ProcedureName);

    FSystem.SysInfoDataset.DeleteRecord(
        'sp:' + DatabaseName + ':' + ProcedureName);
  finally
    FExecLock.Release;
  end;
end;



end.

