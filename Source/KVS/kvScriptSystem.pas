{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/10  0.01  Initial development }
{ 2018/02/28  0.02  Improve usage of selected database/dataset }
{ 2018/03/01  0.03  Persistent stored procedures and related SysInfo }
{ 2018/03/12  0.04  Improve locking }

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
  { TkvScriptSession }

  EkvSession = class(EkvObject);

  TkvScriptSession = class;
  TkvScriptDatabase = class;
  TkvScriptSystemScope = class;
  TkvScriptSystem = class;

  TkvScriptSessionScope = class(AkvScriptScope)
  private
    FSession     : TkvScriptSession;
    FParentScope : TkvScriptSystemScope;
    FIdentifiers : TkvStringHashList;

  public
    constructor Create(const Session: TkvScriptSession;
                const ParentScope: TkvScriptSystemScope);
    destructor Destroy; override;

    function  GetIdentifier(const Identifier: String): TObject; override;
    procedure SetIdentifier(const Identifier: String; const Value: TObject); override;
  end;

  TkvScriptSession = class(AkvScriptSession)
  private
    FSystem : TkvScriptSystem;

    FLocalScope             : TkvScriptSessionScope;
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

    procedure ExecLock; override;
    procedure ExecUnlock; override;

    function  AllocateSystemUniqueId: UInt64; override;

    function  CreateDatabase(const Name: String): TkvDatabase; override;
    procedure DropDatabase(const Name: String); override;
    function  ListOfDatabases: TkvDictionaryValue; override;

    function  AllocateDatabaseUniqueId(const DatabaseName: String): UInt64; override;

    function  CreateDataset(const DatabaseName, DatasetName: String;
              const UseFolders: Boolean): TkvDataset; override;
    procedure DropDataset(const DatabaseName, DatasetName: String); override;
    function  ListOfDatasets(const DatabaseName: String): TkvDictionaryValue; override;

    function  AllocateDatasetUniqueId(const DatabaseName, DatasetName: String): UInt64; override;

    procedure UseDatabase(const Name: String); override;
    procedure UseDataset(const DatabaseName, DatasetName: String); override;
    procedure UseNone; override;
    function  GetSelectedDatabaseName: String; override;
    function  GetSelectedDatasetName: String; override;

    procedure AddRecord(const DatabaseName, DatasetName, Key: String; const Value: AkvValue); override;
    procedure MakePath(const DatabaseName, DatasetName, KeyPath: String); override;
    function  GetRecord(const DatabaseName, DatasetName, Key: String): AkvValue; override;
    function  ListOfKeys(const DatabaseName, DatasetName, KeyPath: String; const Recurse: Boolean): AkvValue; override;
    function  RecordExists(const DatabaseName, DatasetName, Key: String): Boolean; override;
    procedure DeleteRecord(const DatabaseName, DatasetName, Key: String); override;
    procedure SetRecord(const DatabaseName, DatasetName, Key: String; const Value: AkvValue); override;
    procedure AppendRecord(const DatabaseName, DatasetName, Key: String; const Value: AkvValue); override;

    function  IterateRecords(const DatabaseName, DatasetName: String;
              const Path: String; out Iterator: TkvDatasetIterator): Boolean; override;
    function  IterateNextRecord(var Iterator: TkvDatasetIterator): Boolean; override;
    function  IteratorGetKey(const Iterator: TkvDatasetIterator): String; override;
    function  IteratorGetValue(const Iterator: TkvDatasetIterator): AkvValue; override;
    function  IteratorGetTimestamp(const Iterator: TkvDatasetIterator): Int64; override;

    function  ExecScript(const S: String): AkvValue; override;

    procedure CreateStoredProcedure(const DatabaseName, ProcedureName, Script: String); override;
    procedure DropStoredProcedure(const DatabaseName, ProcedureName: String); override;
  end;

  { TkvScriptStoredProcedure }

  TkvScriptStoredProcedure = class
  private
    FScript    : String;
    FProcValue : TkvScriptProcedureValue;

  public
    constructor Create(const Script: String);
    destructor Destroy; override;
  end;


  { TkvScriptDatabase }

  TkvScriptDatabase = class
  private
    FLock           : TCriticalSection;
    FStoredProcList : TkvStringHashList;

    procedure Lock;
    procedure Unlock;
    function  GetStoredProcByName(const Name: String): TkvScriptStoredProcedure;

  public
    constructor Create;
    destructor Destroy; override;

    procedure AddStoredProc(const Name: String; const Script: String);
    procedure RemoveStoredProc(const Name: String);
  end;



  { TkvScriptSystem }

  TkvScriptSystemScope = class
  private
    FSystem : TkvScriptSystem;

    FIdentifiers : TkvStringHashList;

    function  GetProcValueFromDatabase(const Session: TkvScriptSession;
              const Database: TkvScriptDatabase;
              const Identifier: String;
              out Found: Boolean): TkvScriptProcedureValue;

  public
    constructor Create(const System: TkvScriptSystem);
    destructor Destroy; override;

    function  GetIdentifier(const Session: TkvScriptSession; const Identifier: String): TObject;
  end;

  TkvScriptSystem = class
  private
    FSystem : TkvSystem;

    FScope        : TkvScriptSystemScope;
    FSessionLock  : TCriticalSection;
    FSessionList  : array of TkvScriptSession;
    FExecLock     : TCriticalSection;
    FDatabaseList : TkvStringHashList;

    procedure SessionLock;
    procedure SessionUnlock;
    procedure ExecLock;
    procedure ExecUnlock;

    function  GetSessionIndex(const Session: TkvScriptSession): Integer;
    procedure RemoveSessionByIndex(const Index: Integer);
    procedure RemoveSession(const Session: TkvScriptSession);

    function  GetScriptDatabaseByName(const Name: String): TkvScriptDatabase;
    function  AddScriptDatabase(const Name: String): TkvScriptDatabase;
    function  GetOrAddScriptDatabase(const Name: String): TkvScriptDatabase;
    procedure RemoveScriptDatabase(const Name: String);

    procedure LoadSysInfo;

    procedure DropDatabaseStoredProcedures(const DatabaseName: String);

  protected
    procedure SessionClose(const Session: TkvScriptSession);

    function  AllocateSystemUniqueId(const Session: TkvScriptSession): UInt64;

    function  CreateDatabase(const Session: TkvScriptSession; const Name: String): TkvDatabase;
    procedure DropDatabase(const Session: TkvScriptSession; const Name: String);
    function  ListOfDatabases(const Session: TkvScriptSession): TkvDictionaryValue;

    function  AllocateDatabaseUniqueId(const Session: TkvScriptSession; const DatabaseName: String): UInt64;

    function  CreateDataset(const Session: TkvScriptSession; const DatabaseName, DatasetName: String;
              const UseFolders: Boolean): TkvDataset;
    procedure DropDataset(const Session: TkvScriptSession; const DatabaseName, DatasetName: String);
    function  ListOfDatasets(const Session: TkvScriptSession;
              const DatabaseName: String): TkvDictionaryValue;

    function  AllocateDatasetUniqueId(const Session: TkvScriptSession; const DatabaseName, DatasetName: String): UInt64;

    procedure UseDatabase(const Session: TkvScriptSession; const Name: String;
              out Database: TkvDatabase; out ScriptDatabase: TkvScriptDatabase);
    procedure UseDataset(const Session: TkvScriptSession; const DatabaseName, DatasetName: String;
              out Database: TkvDatabase; out ScriptDatabase: TkvScriptDatabase;
              out Dataset: TkvDataset);
    procedure UseNone(const Session: TkvScriptSession);

    procedure AddRecord(const Session: TkvScriptSession;
              const DatabaseName, DatasetName, Key: String;
              const Value: AkvValue); overload;
    procedure AddRecord(const Session: TkvScriptSession; const Dataset: TkvDataset;
              const Key: String; const Value: AkvValue); overload;

    procedure MakePath(const Session: TkvScriptSession;
              const DatabaseName, DatasetName, KeyPath: String); overload;
    procedure MakePath(const Session: TkvScriptSession; const Dataset: TkvDataset;
              const KeyPath: String); overload;

    function  RecordExists(const Session: TkvScriptSession;
              const DatabaseName, DatasetName, Key: String): Boolean; overload;
    function  RecordExists(const Session: TkvScriptSession; const Dataset: TkvDataset;
              const Key: String): Boolean; overload;

    function  GetRecord(const Session: TkvScriptSession;
              const DatabaseName, DatasetName, Key: String): AkvValue; overload;
    function  GetRecord(const Session: TkvScriptSession; const Dataset: TkvDataset;
              const Key: String): AkvValue; overload;

    function  ListOfKeys(const Session: TkvScriptSession;
              const DatabaseName, DatasetName, KeyPath: String;
              const Recurse: Boolean): AkvValue;

    procedure DeleteRecord(const Session: TkvScriptSession;
              const DatabaseName, DatasetName, Key: String); overload;
    procedure DeleteRecord(const Session: TkvScriptSession; const Dataset: TkvDataset;
              const Key: String); overload;

    procedure SetRecord(const Session: TkvScriptSession;
              const DatabaseName, DatasetName, Key: String;
              const Value: AkvValue); overload;
    procedure SetRecord(const Session: TkvScriptSession; const Dataset: TkvDataset;
              const Key: String; const Value: AkvValue); overload;

    procedure AppendRecord(const Session: TkvScriptSession;
              const DatabaseName, DatasetName, Key: String;
              const Value: AkvValue); overload;
    procedure AppendRecord(const Session: TkvScriptSession; const Dataset: TkvDataset;
              const Key: String; const Value: AkvValue); overload;

    function  IterateRecords(const Session: TkvScriptSession;
              const DatabaseName, DatasetName: String;
              const Path: String;
              out Iterator: TkvDatasetIterator): Boolean;
    function  IterateNextRecord(const Session: TkvScriptSession; var Iterator: TkvDatasetIterator): Boolean;
    function  IteratorGetKey(const Session: TkvScriptSession; const Iterator: TkvDatasetIterator): String;
    function  IteratorGetValue(const Session: TkvScriptSession; const Iterator: TkvDatasetIterator): AkvValue;
    function  IteratorGetTimestamp(const Session: TkvScriptSession; const Iterator: TkvDatasetIterator): Int64;

    procedure CreateStoredProcedure(const Session: TkvScriptSession;
              const DatabaseName, ProcedureName, Script: String);
    procedure DropStoredProcedure(const Session: TkvScriptSession;
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
    function  AddSession: TkvScriptSession;
  end;



implementation

uses
  kvScriptFunctions;



{ TkvScriptSessionScope }

constructor TkvScriptSessionScope.Create(const Session: TkvScriptSession;
            const ParentScope: TkvScriptSystemScope);
begin
  Assert(Assigned(Session));
  Assert(Assigned(ParentScope));
  inherited Create;
  FSession := Session;
  FParentScope := ParentScope;
  FIdentifiers := TkvStringHashList.Create(False, False, True);
end;

destructor TkvScriptSessionScope.Destroy;
begin
  FreeAndNil(FIdentifiers);
  inherited Destroy;
end;

function TkvScriptSessionScope.GetIdentifier(const Identifier: String): TObject;
var
  F : Boolean;
  R : TObject;
begin
  F := FIdentifiers.GetValue(Identifier, R);
  if not F then
    R := FParentScope.GetIdentifier(FSession, Identifier);
  Result := R;
end;

procedure TkvScriptSessionScope.SetIdentifier(const Identifier: String;
          const Value: TObject);
begin
  if FIdentifiers.KeyExists(Identifier) then
    FIdentifiers.SetValue(Identifier, Value)
  else
    FIdentifiers.Add(Identifier, Value)
end;



{ TkvScriptSession }

constructor TkvScriptSession.Create(const System: TkvScriptSystem);
begin
  inherited Create;
  FSystem := System;
  FLocalScope := TkvScriptSessionScope.Create(self, System.FScope);
  FContext := TkvScriptContext.Create(FLocalScope, sstGlobal, self);
end;

destructor TkvScriptSession.Destroy;
begin
  FreeAndNil(FContext);
  FreeAndNil(FLocalScope);
  FreeAndNil(FScriptParser);
  inherited Destroy;
end;

procedure TkvScriptSession.Close;
begin
  FSystem.SessionClose(self);
end;

procedure TkvScriptSession.ExecLock;
begin
  FSystem.ExecLock;
end;

procedure TkvScriptSession.ExecUnlock;
begin
  FSystem.ExecUnlock;
end;

function TkvScriptSession.AllocateSystemUniqueId: UInt64;
begin
  Result := FSystem.AllocateSystemUniqueId(self);
end;

function TkvScriptSession.CreateDatabase(const Name: String): TkvDatabase;
begin
  Result := FSystem.CreateDatabase(self, Name);
end;

procedure TkvScriptSession.DropDatabase(const Name: String);
begin
  FSystem.DropDatabase(self, Name);
end;

function TkvScriptSession.ListOfDatabases: TkvDictionaryValue;
begin
  Result := FSystem.ListOfDatabases(self);
end;

function TkvScriptSession.AllocateDatabaseUniqueId(const DatabaseName: String): UInt64;
begin
  Result := FSystem.AllocateDatabaseUniqueId(self, DatabaseName);
end;

function TkvScriptSession.CreateDataset(const DatabaseName, DatasetName: String;
         const UseFolders: Boolean): TkvDataset;
begin
  Result := FSystem.CreateDataset(self, UsedDatabaseName(DatabaseName), DatasetName,
      UseFolders);
end;

procedure TkvScriptSession.DropDataset(const DatabaseName, DatasetName: String);
begin
  FSystem.DropDataset(self, UsedDatabaseName(DatabaseName), DatasetName);
end;

function TkvScriptSession.ListOfDatasets(const DatabaseName: String): TkvDictionaryValue;
begin
  Result := FSystem.ListOfDatasets(self, DatabaseName);
end;

function TkvScriptSession.AllocateDatasetUniqueId(const DatabaseName, DatasetName: String): UInt64;
begin
  Result := FSystem.AllocateDatasetUniqueId(self, DatabaseName, DatasetName);
end;

procedure TkvScriptSession.UseDatabase(const Name: String);
begin
  FSystem.UseDatabase(self, Name, FSelectedDatabase, FSelectedScriptDatabase);
  FSelectedDataset := nil;
  FSelectedDatabaseName := Name;
  FSelectedDatasetName := '';
end;

procedure TkvScriptSession.UseDataset(const DatabaseName, DatasetName: String);
begin
  FSystem.UseDataset(self, DatabaseName, DatasetName,
      FSelectedDatabase, FSelectedScriptDatabase, FSelectedDataset);
  FSelectedDatabaseName := DatabaseName;
  FSelectedDatasetName := DatasetName;
end;

procedure TkvScriptSession.UseNone;
begin
  FSystem.UseNone(self);
  FSelectedDatabase := nil;
  FSelectedScriptDatabase := nil;
  FSelectedDataset := nil;
  FSelectedDatabaseName := '';
  FSelectedDatasetName := '';
end;

function TkvScriptSession.GetSelectedDatabaseName: String;
begin
  Result := FSelectedDatabaseName;
end;

function TkvScriptSession.GetSelectedDatasetName: String;
begin
  Result := FSelectedDatasetName;
end;

function TkvScriptSession.UsedDatabaseName(const DatabaseName: String): String;
begin
  if DatabaseName = '' then
    Result := FSelectedDatabaseName
  else
    Result := DatabaseName;
end;

function TkvScriptSession.UsedDatasetName(const DatasetName: String): String;
begin
  if DatasetName = '' then
    Result := FSelectedDatasetName
  else
    Result := DatasetName;
end;

procedure TkvScriptSession.AddRecord(const DatabaseName, DatasetName, Key: String;
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

procedure TkvScriptSession.MakePath(const DatabaseName, DatasetName, KeyPath: String);
begin
  if (DatasetName = '') and Assigned(FSelectedDataset) then
    FSystem.MakePath(self, FSelectedDataset, KeyPath)
  else
    FSystem.MakePath(self,
        UsedDatabaseName(DatabaseName),
        UsedDatasetName(DatasetName),
        KeyPath);
end;

function TkvScriptSession.GetRecord(const DatabaseName, DatasetName,
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

function TkvScriptSession.ListOfKeys(const DatabaseName, DatasetName, KeyPath: String;
         const Recurse: Boolean): AkvValue;
begin
  Result := FSystem.ListOfKeys(self,
      UsedDatabaseName(DatabaseName),
      UsedDatasetName(DatasetName),
      KeyPath, Recurse);
end;

function TkvScriptSession.RecordExists(const DatabaseName, DatasetName, Key: String): Boolean;
begin
  if (DatasetName = '') and Assigned(FSelectedDataset) then
    Result := FSystem.RecordExists(self, FSelectedDataset, Key)
  else
    Result := FSystem.RecordExists(self,
        UsedDatabaseName(DatabaseName),
        UsedDatasetName(DatasetName),
        Key);
end;

procedure TkvScriptSession.DeleteRecord(const DatabaseName, DatasetName, Key: String);
begin
  if (DatasetName = '') and Assigned(FSelectedDataset) then
    FSystem.DeleteRecord(self, FSelectedDataset, Key)
  else
    FSystem.DeleteRecord(self,
        UsedDatabaseName(DatabaseName),
        UsedDatasetName(DatasetName),
        Key);
end;

procedure TkvScriptSession.SetRecord(const DatabaseName, DatasetName, Key: String;
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

procedure TkvScriptSession.AppendRecord(const DatabaseName, DatasetName, Key: String;
          const Value: AkvValue);
begin
  if (DatasetName = '') and Assigned(FSelectedDataset) then
    FSystem.AppendRecord(self, FSelectedDataset, Key, Value)
  else
    FSystem.AppendRecord(self,
        UsedDatabaseName(DatabaseName),
        UsedDatasetName(DatasetName),
        Key, Value);
end;

function TkvScriptSession.IterateRecords(const DatabaseName, DatasetName: String;
         const Path: String; out Iterator: TkvDatasetIterator): Boolean;
begin
  Result := FSystem.IterateRecords(self, DatabaseName, DatasetName, Path, Iterator);
end;

function TkvScriptSession.IterateNextRecord(var Iterator: TkvDatasetIterator): Boolean;
begin
  Result := FSystem.IterateNextRecord(self, Iterator);
end;

function TkvScriptSession.IteratorGetKey(const Iterator: TkvDatasetIterator): String;
begin
  Result := FSystem.IteratorGetKey(self, Iterator);
end;

function TkvScriptSession.IteratorGetValue(const Iterator: TkvDatasetIterator): AkvValue;
begin
  Result := FSystem.IteratorGetValue(self, Iterator);
end;

function TkvScriptSession.IteratorGetTimestamp(const Iterator: TkvDatasetIterator): Int64;
begin
  Result := FSystem.IteratorGetTimestamp(self, Iterator);
end;

function TkvScriptSession.ParseScript(const Script: String): AkvScriptNode;
begin
  if not Assigned(FScriptParser) then
    FScriptParser := TkvScriptParser.Create;
  Result := FScriptParser.Parse(Script);
end;

function TkvScriptSession.ExecScript(const S: String): AkvValue;
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

procedure TkvScriptSession.CreateStoredProcedure(const DatabaseName, ProcedureName, Script: String);
begin
  FSystem.CreateStoredProcedure(self, DatabaseName, ProcedureName, Script);
end;

procedure TkvScriptSession.DropStoredProcedure(const DatabaseName, ProcedureName: String);
begin
  FSystem.DropStoredProcedure(self, DatabaseName, ProcedureName);
end;



{ TkvScriptSessionSystemScope }

constructor TkvScriptSystemScope.Create(const System: TkvScriptSystem);
begin
  Assert(Assigned(System));
  inherited Create;
  FSystem := System;
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
  FIdentifiers.Add('GETTIMESTAMP', TkvScriptGetTimestampBuiltInFunction.Create);
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

function TkvScriptSystemScope.GetProcValueFromDatabase(const Session: TkvScriptSession;
         const Database: TkvScriptDatabase;
         const Identifier: String;
         out Found: Boolean): TkvScriptProcedureValue;
var
  SP : TkvScriptStoredProcedure;
  N : AkvScriptNode;
begin
  Found := False;
  Result := nil;
  Database.Lock;
  try
    SP := Database.GetStoredProcByName(Identifier);
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
        Result := SP.FProcValue;
        Found := True;
      end;
  finally
    Database.Unlock;
  end;
end;

function TkvScriptSystemScope.GetIdentifier(const Session: TkvScriptSession;
         const Identifier: String): TObject;
var
  F : Boolean;
  R : TObject;
  SDb : TkvScriptDatabase;
begin
  F := FIdentifiers.GetValue(Identifier, R);
  if not F then
    begin
      SDb := Session.FSelectedScriptDatabase;
      if Assigned(SDb) then
        R := GetProcValueFromDatabase(Session, SDb, Identifier, F);
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
  FLock := TCriticalSection.Create;
  FStoredProcList := TkvStringHashList.Create(False, False, True);
end;

destructor TkvScriptDatabase.Destroy;
begin
  FreeAndNil(FStoredProcList);
  FreeAndNil(FLock);
  inherited Destroy;
end;

procedure TkvScriptDatabase.Lock;
begin
  FLock.Acquire;
end;

procedure TkvScriptDatabase.Unlock;
begin
  FLock.Release;
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
  Lock;
  try
    FStoredProcList.Add(Name, Proc);
  finally
    Unlock;
  end;
end;

procedure TkvScriptDatabase.RemoveStoredProc(const Name: String);
begin
  Lock;
  try
    FStoredProcList.DeleteKey(Name);
  finally
    Unlock;
  end;
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
  FScope := TkvScriptSystemScope.Create(self);
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

procedure TkvScriptSystem.SessionLock;
begin
  FSessionLock.Acquire;
end;

procedure TkvScriptSystem.SessionUnlock;
begin
  FSessionLock.Release;
end;

procedure TkvScriptSystem.ExecLock;
begin
  FExecLock.Acquire;
end;

procedure TkvScriptSystem.ExecUnlock;
begin
  FExecLock.Release;
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
  SessionLock;
  try
    Result := Length(FSessionList);
  finally
    SessionUnlock;
  end;
end;

procedure TkvScriptSystem.ClearSessions;
var
  I : Integer;
begin
  SessionLock;
  try
    for I := Length(FSessionList) - 1 downto 0 do
     FreeAndNil(FSessionList[I]);
    SetLength(FSessionList, 0);
  finally
    SessionUnlock;
  end;
end;

function TkvScriptSystem.AddSession: TkvScriptSession;
var
  S : TkvScriptSession;
  L : Integer;
begin
  S := TkvScriptSession.Create(self);
  SessionLock;
  try
    L := Length(FSessionList);
    SetLength(FSessionList, L + 1);
    FSessionList[L] := S;
  finally
    SessionUnlock;
  end;
  Result := S;
end;

function TkvScriptSystem.GetSessionIndex(const Session: TkvScriptSession): Integer;
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
  Ses : TkvScriptSession;
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

procedure TkvScriptSystem.RemoveSession(const Session: TkvScriptSession);
var
  I : Integer;
begin
  SessionLock;
  try
    I := GetSessionIndex(Session);
    if I < 0 then
      raise EkvObject.Create('Session not found');
    RemoveSessionByIndex(I);
  finally
    SessionUnlock;
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

procedure TkvScriptSystem.SessionClose(const Session: TkvScriptSession);
begin
  RemoveSession(Session);
end;

function TkvScriptSystem.AllocateSystemUniqueId(const Session: TkvScriptSession): UInt64;
begin
  ExecLock;
  try
    Result := FSystem.AllocateSystemUniqueId;
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.CreateDatabase(const Session: TkvScriptSession; const Name: String): TkvDatabase;
begin
  ExecLock;
  try
    Result := FSystem.CreateDatabase(Name);
    FSystem.SysInfoDataset.AddRecordNull('db:' + Name);
    AddScriptDatabase(Name);
  finally
    ExecUnlock;
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
      FSystem.SysInfoDataset.DeleteRecord('sp:' + DatabaseName + ':' + Im^.Key);
      Im := Db.FStoredProcList.IterateNext(It);
    end;
end;

procedure TkvScriptSystem.DropDatabase(const Session: TkvScriptSession; const Name: String);
begin
  ExecLock;
  try
    FSystem.DropDatabase(Name);
    FSystem.SysInfoDataset.DeleteRecord('db:' + Name);
    DropDatabaseStoredProcedures(Name);
    RemoveScriptDatabase(Name);
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.ListOfDatabases(const Session: TkvScriptSession): TkvDictionaryValue;
var
  L, I : Integer;
  R, D : TkvDictionaryValue;
  ItR : Boolean;
  It : TkvDatabaseListIterator;
begin
  ExecLock;
  try
    L := FSystem.GetDatabaseCount;
    R := TkvDictionaryValue.Create;
    I := 0;
    ItR := FSystem.IterateFirstDatabase(It);
    while ItR and (I < L) do
      begin
        D := TkvDictionaryValue.Create;
        R.Add(It.Key, D);
        ItR := FSystem.IterateNextDatabase(It);
        Inc(I);
      end;
  finally
    ExecUnlock;
  end;
  Result := R;
end;

function TkvScriptSystem.AllocateDatabaseUniqueId(const Session: TkvScriptSession;
         const DatabaseName: String): UInt64;
begin
  ExecLock;
  try
    Result := FSystem.AllocateDatabaseUniqueId(DatabaseName);
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.CreateDataset(const Session: TkvScriptSession;
         const DatabaseName, DatasetName: String;
         const UseFolders: Boolean): TkvDataset;
begin
  ExecLock;
  try
    Result := FSystem.CreateDataset(DatabaseName, DatasetName, UseFolders);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.DropDataset(const Session: TkvScriptSession;
          const DatabaseName, DatasetName: String);
begin
  ExecLock;
  try
    FSystem.DropDataset(DatabaseName, DatasetName);
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.ListOfDatasets(const Session: TkvScriptSession;
         const DatabaseName: String): TkvDictionaryValue;
var
  Db : TkvDatabase;
  L, I : Integer;
  ItR : Boolean;
  It : TkvDatasetListIterator;
  R, D : TkvDictionaryValue;
begin
  ExecLock;
  try
    Db := FSystem.RequireDatabaseByName(DatabaseName);
    L := Db.GetDatasetCount;
    R := TkvDictionaryValue.Create;
    I := 0;
    ItR := FSystem.IterateFirstDataset(DatabaseName, It);
    while ItR and (I < L) do
      begin
        D := TkvDictionaryValue.Create;
        D.AddBoolean('with_folders', It.Dataset.UseFolders);
        R.Add(It.Key, D);
        ItR := FSystem.IterateNextDataset(It);
        Inc(I);
      end;
  finally
    ExecUnlock;
  end;
  Result := R;
end;

function TkvScriptSystem.AllocateDatasetUniqueId(const Session: TkvScriptSession;
         const DatabaseName, DatasetName: String): UInt64;
begin
  ExecLock;
  try
    Result := FSystem.AllocateDatasetUniqueId(DatabaseName, DatasetName);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.UseDatabase(const Session: TkvScriptSession;
          const Name: String;
          out Database: TkvDatabase; out ScriptDatabase: TkvScriptDatabase);
begin
  ExecLock;
  try
    if not FSystem.DatabaseExists(Name) then
      raise EkvSession.CreateFmt('Database does not exist: %s', [Name]);
    Database := FSystem.RequireDatabaseByName(Name);
    ScriptDatabase := GetScriptDatabaseByName(Name);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.UseDataset(
          const Session: TkvScriptSession;
          const DatabaseName, DatasetName: String;
          out Database: TkvDatabase; out ScriptDatabase: TkvScriptDatabase;
          out Dataset: TkvDataset);
begin
  ExecLock;
  try
    if not FSystem.DatabaseExists(DatabaseName) then
      raise EkvSession.CreateFmt('Database does not exist: %s', [DatabaseName]);
    if not FSystem.DatasetExists(DatabaseName, DatasetName) then
      raise EkvSession.CreateFmt('Dataset does not exist: %s', [DatasetName]);
    Database := FSystem.RequireDatabaseByName(DatabaseName);
    ScriptDatabase := GetScriptDatabaseByName(DatabaseName);
    Dataset := Database.RequireDatasetByName(DatasetName);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.UseNone(const Session: TkvScriptSession);
begin
  Assert(Assigned(Session));
end;

procedure TkvScriptSystem.AddRecord(const Session: TkvScriptSession;
          const DatabaseName, DatasetName, Key: String; const Value: AkvValue);
begin
  ExecLock;
  try
    FSystem.AddRecord(DatabaseName, DatasetName, Key, Value);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.AddRecord(const Session: TkvScriptSession;
          const Dataset: TkvDataset; const Key: String; const Value: AkvValue);
begin
  ExecLock;
  try
    FSystem.AddRecord(Dataset, Key, Value);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.MakePath(const Session: TkvScriptSession; const DatabaseName, DatasetName, KeyPath: String);
begin
  ExecLock;
  try
    FSystem.MakePath(DatabaseName, DatasetName, KeyPath);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.MakePath(const Session: TkvScriptSession; const Dataset: TkvDataset;
          const KeyPath: String);
begin
  ExecLock;
  try
    FSystem.MakePath(Dataset, KeyPath);
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.RecordExists(const Session: TkvScriptSession;
         const DatabaseName, DatasetName, Key: String): Boolean;
begin
  ExecLock;
  try
    Result := FSystem.RecordExists(DatabaseName, DatasetName, Key);
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.RecordExists(const Session: TkvScriptSession;
         const Dataset: TkvDataset; const Key: String): Boolean;
begin
  ExecLock;
  try
    Result := FSystem.RecordExists(Dataset, Key);
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.GetRecord(const Session: TkvScriptSession;
         const DatabaseName, DatasetName, Key: String): AkvValue;
begin
  ExecLock;
  try
    Result := FSystem.GetRecord(DatabaseName, DatasetName, Key);
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.GetRecord(const Session: TkvScriptSession;
         const Dataset: TkvDataset; const Key: String): AkvValue;
begin
  ExecLock;
  try
    Result := FSystem.GetRecord(Dataset, Key);
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.ListOfKeys(const Session: TkvScriptSession;
         const DatabaseName, DatasetName, KeyPath: String;
         const Recurse: Boolean): AkvValue;
begin
  ExecLock;
  try
    Result := FSystem.ListOfKeys(DatabaseName, DatasetName, KeyPath, Recurse);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.DeleteRecord(const Session: TkvScriptSession;
          const DatabaseName, DatasetName, Key: String);
begin
  ExecLock;
  try
    FSystem.DeleteRecord(DatabaseName, DatasetName, Key);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.DeleteRecord(const Session: TkvScriptSession;
          const Dataset: TkvDataset;
          const Key: String);
begin
  ExecLock;
  try
    FSystem.DeleteRecord(Dataset, Key);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.SetRecord(const Session: TkvScriptSession;
          const DatabaseName, DatasetName, Key: String; const Value: AkvValue);
begin
  ExecLock;
  try
    FSystem.SetRecord(DatabaseName, DatasetName, Key, Value);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.SetRecord(const Session: TkvScriptSession;
          const Dataset: TkvDataset;
          const Key: String; const Value: AkvValue);
begin
  ExecLock;
  try
    FSystem.SetRecord(Dataset, Key, Value);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.AppendRecord(const Session: TkvScriptSession;
          const DatabaseName, DatasetName, Key: String;
          const Value: AkvValue);
begin
  ExecLock;
  try
    FSystem.AppendRecord(DatabaseName, DatasetName, Key, Value);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.AppendRecord(const Session: TkvScriptSession;
          const Dataset: TkvDataset;
          const Key: String; const Value: AkvValue);
begin
  ExecLock;
  try
    FSystem.AppendRecord(Dataset, Key, Value);
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.IterateRecords(const Session: TkvScriptSession;
         const DatabaseName, DatasetName: String;
         const Path: String;
         out Iterator: TkvDatasetIterator): Boolean;
begin
  ExecLock;
  try
    Result := FSystem.IterateRecords(DatabaseName, DatasetName, Path, Iterator);
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.IterateNextRecord(const Session: TkvScriptSession;
         var Iterator: TkvDatasetIterator): Boolean;
begin
  ExecLock;
  try
    Result := FSystem.IterateNextRecord(Iterator);
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.IteratorGetKey(const Session: TkvScriptSession;
         const Iterator: TkvDatasetIterator): String;
begin
  ExecLock;
  try
    Result := FSystem.IteratorGetKey(Iterator);
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.IteratorGetValue(const Session: TkvScriptSession;
         const Iterator: TkvDatasetIterator): AkvValue;
begin
  ExecLock;
  try
    Result := FSystem.IteratorGetValue(Iterator);
  finally
    ExecUnlock;
  end;
end;

function TkvScriptSystem.IteratorGetTimestamp(const Session: TkvScriptSession;
         const Iterator: TkvDatasetIterator): Int64;
begin
  ExecLock;
  try
    Result := FSystem.IteratorGetTimestamp(Iterator);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.CreateStoredProcedure(const Session: TkvScriptSession;
          const DatabaseName, ProcedureName, Script: String);
var
  Db : TkvScriptDatabase;
begin
  Assert(DatabaseName <> '');
  ExecLock;
  try
    Db := GetOrAddScriptDatabase(DatabaseName);
    Db.AddStoredProc(ProcedureName, Script);

    FSystem.SysInfoDataset.AddRecordString(
        'sp:' + DatabaseName + ':' + ProcedureName,
        Script);
  finally
    ExecUnlock;
  end;
end;

procedure TkvScriptSystem.DropStoredProcedure(const Session: TkvScriptSession;
          const DatabaseName, ProcedureName: String);
var
  Db : TkvScriptDatabase;
begin
  ExecLock;
  try
    Db := GetScriptDatabaseByName(DatabaseName);
    if not Assigned(Db) then
      raise EkvSession.CreateFmt('Database not found: %s', [DatabaseName]);
    Db.RemoveStoredProc(ProcedureName);

    FSystem.SysInfoDataset.DeleteRecord(
        'sp:' + DatabaseName + ':' + ProcedureName);
  finally
    ExecUnlock;
  end;
end;



end.

