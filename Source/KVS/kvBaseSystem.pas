{ KeyVast - A key value store }
{ Copyright (c) 2018-2019 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2019/10/03  0.01  Initial version based on DiskSystem. }

{$INCLUDE kvInclude.inc}

unit kvBaseSystem;

interface

uses
  System.SysUtils,

  kvValues,
  kvAbstractSystem;



type
  EkvBaseDataset = class(Exception);

  AkvBaseDataset = class(AkvDataset)
  public
    function  GetRecordAsString(const AKey: String): String;
    function  GetRecordAsInteger(const AKey: String): Int64;
    function  GetRecordAsFloat(const AKey: String): Double;
    function  GetRecordAsBoolean(const AKey: String): Boolean;
    function  GetRecordAsDateTime(const AKey: String): TDateTime;
    function  GetRecordAsBinary(const AKey: String): kvByteArray;
    function  GetRecordIsNull(const AKey: String): Boolean;

    function  GetRecordAsStringDef(const AKey: String; const ADefaultValue: String = ''): String;
    function  GetRecordAsIntegerDef(const AKey: String; const ADefaultValue: Int64 = 0): Int64;
    function  GetRecordAsFloatDef(const AKey: String; const ADefaultValue: Double = 0.0): Double;
    function  GetRecordAsBooleanDef(const AKey: String; const ADefaultValue: Boolean = False): Boolean;
    function  GetRecordAsDateTimeDef(const AKey: String; const ADefaultValue: TDateTime = 0.0): TDateTime;
    function  GetRecordAsBinaryDef(const AKey: String): kvByteArray;

    procedure AddRecordString(const AKey: String; const AValue: String);
    procedure AddRecordInteger(const AKey: String; const AValue: Int64);
    procedure AddRecordFloat(const AKey: String; const AValue: Double);
    procedure AddRecordBoolean(const AKey: String; const AValue: Boolean);
    procedure AddRecordDateTime(const AKey: String; const AValue: TDateTime);
    procedure AddRecordBinary(const AKey: String; const AValue: kvByteArray);
    procedure AddRecordNull(const AKey: String);

    procedure SetRecordAsString(const AKey: String; const AValue: String);
    procedure SetRecordAsInteger(const AKey: String; const AValue: Int64);
    procedure SetRecordAsFloat(const AKey: String; const AValue: Double);
    procedure SetRecordAsBoolean(const AKey: String; const AValue: Boolean);
    procedure SetRecordAsDateTime(const AKey: String; const AValue: TDateTime);
    procedure SetRecordAsBinary(const AKey: String; const AValue: kvByteArray);
    procedure SetRecordNull(const AKey: String);

    procedure SetOrAddRecordAsString(const AKey: String; const AValue: String);
    procedure SetOrAddRecordAsInteger(const AKey: String; const AValue: Int64);
    procedure SetOrAddRecordAsFloat(const AKey: String; const AValue: Double);
    procedure SetOrAddRecordAsBoolean(const AKey: String; const AValue: Boolean);
    procedure SetOrAddRecordAsDateTime(const AKey: String; const AValue: TDateTime);
    procedure SetOrAddRecordAsBinary(const AKey: String; const AValue: kvByteArray);

    procedure AppendRecordString(const AKey: String; const AValue: String);
  end;

  AkvBaseDatabase = class(AkvDatabase)
  end;

  AkvBaseSystem = class(AkvSystem)
  public
    function  AllocateDatabaseUniqueId(const ADatabaseName: String): UInt64;

    function  DatasetExists(const ADatabaseName, ADatasetName: String): Boolean;
    function  RequireDatasetByName(const ADatabaseName, ADatasetName: String): AkvDataset;
    function  CreateDataset(
              const ADatabaseName, ADatasetName: String;
              const AUseFolders: Boolean): AkvDataset;
    procedure DropDataset(const ADatabaseName, ADatasetName: String);
    function  IterateFirstDataset(const ADatabaseName: String;
              var AIterator: AkvDatasetListIterator): Boolean;
    function  IterateNextDataset(var AIterator: AkvDatasetListIterator): Boolean;

    function  AllocateDatasetUniqueId(const ADatabaseName, ADatasetName: String): UInt64;

    function  RecordExists(const ADatabaseName, ADatasetName, AKey: String): Boolean; overload;
    function  GetRecord(const ADatabaseName, ADatasetName, AKey: String): AkvValue; overload;
    procedure MakePath(const ADatabaseName, ADatasetName, AKeyPath: String); overload;
    procedure AddRecord(const ADatabaseName, ADatasetName, AKey: String;
              const AValue: AkvValue); overload;
    procedure SetRecord(const ADatabaseName, ADatasetName, AKey: String;
              const AValue: AkvValue); overload;
    procedure AppendRecord(const ADatabaseName, ADatasetName, AKey: String;
              const AValue: AkvValue); overload;
    procedure DeleteRecord(const ADatabaseName, ADatasetName, AKey: String); overload;

    function  ListOfKeys(const ADatabaseName, ADatasetName, AKeyPath: String;
              const ARecurse: Boolean): AkvValue;
    function  IterateRecords(const ADatabaseName, ADatasetName: String;
              const APath: String;
              out AIterator: AkvDatasetIterator;
              const ARecurse: Boolean;
              const AMinTimestamp: UInt64): Boolean;
    function  IterateFolders(const ADatabaseName, ADatasetName: String;
              const APath: String;
              out AIterator: AkvDatasetIterator): Boolean;

    function  IterateNextRecord(var AIterator: AkvDatasetIterator): Boolean;
    function  IteratorGetKey(const AIterator: AkvDatasetIterator): String;
    function  IteratorGetValue(const AIterator: AkvDatasetIterator): AkvValue;
    function  IteratorGetTimestamp(const AIterator: AkvDatasetIterator): Int64;

    function  RecordExists(const ADataset: AkvDataset; const AKey: String): Boolean; overload;
    function  GetRecord(const ADataset: AkvDataset; const AKey: String): AkvValue; overload;
    procedure MakePath(const ADataset: AkvDataset; const AKeyPath: String); overload;
    procedure AddRecord(const ADataset: AkvDataset; const AKey: String;
              const AValue: AkvValue); overload;
    procedure SetRecord(const ADataset: AkvDataset; const AKey: String;
              const AValue: AkvValue); overload;
    procedure AppendRecord(const ADataset: AkvDataset; const AKey: String;
              const AValue: AkvValue); overload;
    procedure DeleteRecord(const ADataset: AkvDataset; const AKey: String); overload;
  end;



implementation



function AkvBaseDataset.GetRecordAsString(const AKey: String): String;
var
  V : AkvValue;
begin
  V := GetRecord(AKey);
  try
    Result := V.AsString;
  finally
    V.Free;
  end;
end;

function AkvBaseDataset.GetRecordAsInteger(const AKey: String): Int64;
var
  V : AkvValue;
begin
  V := GetRecord(AKey);
  try
    Result := V.AsInteger;
  finally
    V.Free;
  end;
end;

function AkvBaseDataset.GetRecordAsFloat(const AKey: String): Double;
var
  V : AkvValue;
begin
  V := GetRecord(AKey);
  try
    Result := V.AsFloat;
  finally
    V.Free;
  end;
end;

function AkvBaseDataset.GetRecordAsBoolean(const AKey: String): Boolean;
var
  V : AkvValue;
begin
  V := GetRecord(AKey);
  try
    Result := V.AsBoolean;
  finally
    V.Free;
  end;
end;

function AkvBaseDataset.GetRecordAsDateTime(const AKey: String): TDateTime;
var
  V : AkvValue;
begin
  V := GetRecord(AKey);
  try
    Result := V.AsDateTime;
  finally
    V.Free;
  end;
end;

function AkvBaseDataset.GetRecordAsBinary(const AKey: String): kvByteArray;
var
  V : AkvValue;
begin
  V := GetRecord(AKey);
  try
    Result := V.AsBinary;
  finally
    V.Free;
  end;
end;

function AkvBaseDataset.GetRecordIsNull(const AKey: String): Boolean;
var
  V : AkvValue;
begin
  V := GetRecord(AKey);
  try
    Result := V is TkvNullValue;
  finally
    V.Free;
  end;
end;

function AkvBaseDataset.GetRecordAsStringDef(const AKey: String; const ADefaultValue: String): String;
var
  Val : AkvValue;
begin
  Val := GetRecordIfExists(AKey);
  if not Assigned(Val) then
    Result := ADefaultValue
  else
  if Val is TkvNullValue then
    Result := ADefaultValue
  else
    try
      Result := Val.AsString;
    except
      Result := ADefaultValue;
    end;
end;

function AkvBaseDataset.GetRecordAsIntegerDef(const AKey: String; const ADefaultValue: Int64): Int64;
var
  Val : AkvValue;
begin
  Val := GetRecordIfExists(AKey);
  if not Assigned(Val) then
    Result := ADefaultValue
  else
  if Val is TkvNullValue then
    Result := ADefaultValue
  else
    try
      Result := Val.AsInteger;
    except
      Result := ADefaultValue;
    end;
end;

function AkvBaseDataset.GetRecordAsFloatDef(const AKey: String; const ADefaultValue: Double): Double;
var
  Val : AkvValue;
begin
  Val := GetRecordIfExists(AKey);
  if not Assigned(Val) then
    Result := ADefaultValue
  else
  if Val is TkvNullValue then
    Result := ADefaultValue
  else
    try
      Result := Val.AsFloat;
    except
      Result := ADefaultValue;
    end;
end;

function AkvBaseDataset.GetRecordAsBooleanDef(const AKey: String; const ADefaultValue: Boolean): Boolean;
var
  Val : AkvValue;
begin
  Val := GetRecordIfExists(AKey);
  if not Assigned(Val) then
    Result := ADefaultValue
  else
  if Val is TkvNullValue then
    Result := ADefaultValue
  else
    try
      Result := Val.AsBoolean;
    except
      Result := ADefaultValue;
    end;
end;

function AkvBaseDataset.GetRecordAsDateTimeDef(const AKey: String; const ADefaultValue: TDateTime): TDateTime;
var
  Val : AkvValue;
begin
  Val := GetRecordIfExists(AKey);
  if not Assigned(Val) then
    Result := ADefaultValue
  else
  if Val is TkvNullValue then
    Result := ADefaultValue
  else
    try
      Result := Val.AsDateTime;
    except
      Result := ADefaultValue;
    end;
end;

function AkvBaseDataset.GetRecordAsBinaryDef(const AKey: String): kvByteArray;
var
  Val : AkvValue;
begin
  Val := GetRecordIfExists(AKey);
  if not Assigned(Val) then
    Result := nil
  else
  if Val is TkvNullValue then
    Result := nil
  else
    try
      Result := Val.AsBinary;
    except
      Result := nil;
    end;
end;

procedure AkvBaseDataset.AddRecordString(const AKey: String; const AValue: String);
var
  V : TkvStringValue;
begin
  V := TkvStringValue.Create(AValue);
  try
    AddRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.AddRecordInteger(const AKey: String; const AValue: Int64);
var
  V : TkvIntegerValue;
begin
  V := TkvIntegerValue.Create(AValue);
  try
    AddRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.AddRecordFloat(const AKey: String; const AValue: Double);
var
  V : TkvFloatValue;
begin
  V := TkvFloatValue.Create(AValue);
  try
    AddRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.AddRecordBoolean(const AKey: String; const AValue: Boolean);
var
  V : TkvBooleanValue;
begin
  V := TkvBooleanValue.Create(AValue);
  try
    AddRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.AddRecordDateTime(const AKey: String; const AValue: TDateTime);
var
  V : TkvDateTimeValue;
begin
  V := TkvDateTimeValue.Create(AValue);
  try
    AddRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.AddRecordBinary(const AKey: String; const AValue: kvByteArray);
var
  V : TkvBinaryValue;
begin
  V := TkvBinaryValue.Create(AValue);
  try
    AddRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.AddRecordNull(const AKey: String);
var
  V : TkvNullValue;
begin
  V := TkvNullValue.Create;
  try
    AddRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.SetRecordAsString(const AKey: String; const AValue: String);
var
  V : TkvStringValue;
begin
  V := TkvStringValue.Create(AValue);
  try
    SetRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.SetRecordAsInteger(const AKey: String; const AValue: Int64);
var
  V : TkvIntegerValue;
begin
  V := TkvIntegerValue.Create(AValue);
  try
    SetRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.SetRecordAsFloat(const AKey: String; const AValue: Double);
var
  V : TkvFloatValue;
begin
  V := TkvFloatValue.Create(AValue);
  try
    SetRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.SetRecordAsBoolean(const AKey: String; const AValue: Boolean);
var
  V : TkvBooleanValue;
begin
  V := TkvBooleanValue.Create(AValue);
  try
    SetRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.SetRecordAsDateTime(const AKey: String; const AValue: TDateTime);
var
  V : TkvDateTimeValue;
begin
  V := TkvDateTimeValue.Create(AValue);
  try
    SetRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.SetRecordAsBinary(const AKey: String; const AValue: kvByteArray);
var
  V : TkvBinaryValue;
begin
  V := TkvBinaryValue.Create(AValue);
  try
    SetRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.SetRecordNull(const AKey: String);
var
  V : TkvNullValue;
begin
  V := TkvNullValue.Create;
  try
    SetRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.SetOrAddRecordAsString(const AKey: String; const AValue: String);
var
  V : TkvStringValue;
begin
  V := TkvStringValue.Create(AValue);
  try
    SetOrAddRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.SetOrAddRecordAsInteger(const AKey: String; const AValue: Int64);
var
  V : TkvIntegerValue;
begin
  V := TkvIntegerValue.Create(AValue);
  try
    SetOrAddRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.SetOrAddRecordAsFloat(const AKey: String; const AValue: Double);
var
  V : TkvFloatValue;
begin
  V := TkvFloatValue.Create(AValue);
  try
    SetOrAddRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.SetOrAddRecordAsBoolean(const AKey: String; const AValue: Boolean);
var
  V : TkvBooleanValue;
begin
  V := TkvBooleanValue.Create(AValue);
  try
    SetOrAddRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.SetOrAddRecordAsDateTime(const AKey: String; const AValue: TDateTime);
var
  V : TkvDateTimeValue;
begin
  V := TkvDateTimeValue.Create(AValue);
  try
    SetOrAddRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.SetOrAddRecordAsBinary(const AKey: String; const AValue: kvByteArray);
var
  V : TkvBinaryValue;
begin
  V := TkvBinaryValue.Create(AValue);
  try
    SetOrAddRecord(AKey, V);
  finally
    V.Free;
  end;
end;

procedure AkvBaseDataset.AppendRecordString(const AKey: String; const AValue: String);
var
  V : TkvStringValue;
begin
  V := TkvStringValue.Create(AValue);
  try
    AppendRecord(AKey, V);
  finally
    V.Free;
  end;
end;



{ AkvBaseSystem }

function AkvBaseSystem.AllocateDatabaseUniqueId(const ADatabaseName: String): UInt64;
var
  Db : AkvDatabase;
begin
  Db := RequireDatabaseByName(ADatabaseName);
  Result := Db.AllocateUniqueId;
end;

function AkvBaseSystem.DatasetExists(const ADatabaseName, ADatasetName: String): Boolean;
var
  Db : AkvDatabase;
begin
  Db := RequireDatabaseByName(ADatabaseName);
  Result := Db.DatasetExists(ADatasetName);
end;

function AkvBaseSystem.RequireDatasetByName(const ADatabaseName, ADatasetName: String): AkvDataset;
var
  Db : AkvDatabase;
begin
  Db := RequireDatabaseByName(ADatabaseName);
  Result := Db.RequireDatasetByName(ADatasetName);
end;

function AkvBaseSystem.CreateDataset(
         const ADatabaseName, ADatasetName: String;
         const AUseFolders: Boolean): AkvDataset;
var
  Db : AkvDatabase;
  Ds : AkvDataset;
begin
  if ADatasetName = '' then
    raise EkvBaseDataset.Create('Dataset name required');

  Db := RequireDatabaseByName(ADatabaseName);
  if Db.DatasetExists(ADatasetName) then
    raise EkvBaseDataset.CreateFmt('Dataset exists: %s:%s', [ADatabaseName, ADatasetName]);

  Ds := Db.AddDataset(ADatasetName, AUseFolders);
  Result := Ds;
end;

procedure AkvBaseSystem.DropDataset(const ADatabaseName, ADatasetName: String);
var
  Db : AkvDatabase;
begin
  Db := RequireDatabaseByName(ADatabaseName);
  Db.RequireDatasetByName(ADatasetName);
  Db.RemoveDataset(ADatasetName);
end;

function AkvBaseSystem.IterateFirstDataset(const ADatabaseName: String;
         var AIterator: AkvDatasetListIterator): Boolean;
var
  Db : AkvDatabase;
begin
  Db := RequireDatabaseByName(ADatabaseName);
  Result := Db.IterateFirstDataset(AIterator);
end;

function AkvBaseSystem.IterateNextDataset(var AIterator: AkvDatasetListIterator): Boolean;
begin
  Assert(AIterator.GetDatabase <> nil);
  Result := AIterator.GetDatabase.IterateNextDataset(AIterator);
end;

function AkvBaseSystem.AllocateDatasetUniqueId(const ADatabaseName, ADatasetName: String): UInt64;
var
  Db : AkvDatabase;
  Ds : AkvDataset;
begin
  Db := RequireDatabaseByName(ADatabaseName);
  Ds := Db.RequireDatasetByName(ADatasetName);
  Result := Ds.AllocateUniqueId;
end;

function AkvBaseSystem.RecordExists(const ADatabaseName, ADatasetName, AKey: String): Boolean;
begin
  Result := RequireDatasetByName(ADatabaseName, ADatasetName).RecordExists(AKey);
end;

function AkvBaseSystem.GetRecord(const ADatabaseName, ADatasetName, AKey: String): AkvValue;
begin
  Result := RequireDatasetByName(ADatabaseName, ADatasetName).GetRecord(AKey);
end;

procedure AkvBaseSystem.MakePath(const ADatabaseName, ADatasetName, AKeyPath: String);
begin
  RequireDatasetByName(ADatabaseName, ADatasetName).MakePath(AKeyPath);
end;

procedure AkvBaseSystem.AddRecord(const ADatabaseName, ADatasetName, AKey: String;
          const AValue: AkvValue);
begin
  RequireDatasetByName(ADatabaseName, ADatasetName).AddRecord(AKey, AValue);
end;

procedure AkvBaseSystem.SetRecord(const ADatabaseName, ADatasetName, AKey: String;
          const AValue: AkvValue);
begin
  RequireDatasetByName(ADatabaseName, ADatasetName).SetRecord(AKey, AValue);
end;

procedure AkvBaseSystem.AppendRecord(const ADatabaseName, ADatasetName, AKey: String;
          const AValue: AkvValue);
begin
  RequireDatasetByName(ADatabaseName, ADatasetName).AppendRecord(AKey, AValue);
end;

procedure AkvBaseSystem.DeleteRecord(const ADatabaseName, ADatasetName, AKey: String);
begin
  RequireDatasetByName(ADatabaseName, ADatasetName).DeleteRecord(AKey);
end;

function AkvBaseSystem.ListOfKeys(const ADatabaseName, ADatasetName, AKeyPath: String;
         const ARecurse: Boolean): AkvValue;
begin
  Result := RequireDatasetByName(ADatabaseName, ADatasetName).ListOfKeys(AKeyPath, ARecurse);
end;

function AkvBaseSystem.IterateRecords(const ADatabaseName, ADatasetName: String;
         const APath: String;
         out AIterator: AkvDatasetIterator;
         const ARecurse: Boolean;
         const AMinTimestamp: UInt64): Boolean;
begin
  Result := RequireDatasetByName(ADatabaseName, ADatasetName).IterateRecords(APath, AIterator, ARecurse, AMinTimestamp);
end;

function AkvBaseSystem.IterateFolders(const ADatabaseName, ADatasetName: String;
         const APath: String;
         out AIterator: AkvDatasetIterator): Boolean;
begin
  Result := RequireDatasetByName(ADatabaseName, ADatasetName).IterateFolders(APath, AIterator);
end;

function AkvBaseSystem.IterateNextRecord(var AIterator: AkvDatasetIterator): Boolean;
begin
  Assert(Assigned(AIterator));
  Assert(AIterator.GetDataset <> nil);
  Result := AIterator.GetDataset.IterateNextRecord(AIterator);
end;

function AkvBaseSystem.IteratorGetKey(const AIterator: AkvDatasetIterator): String;
begin
  Assert(Assigned(AIterator));
  Assert(AIterator.GetDataset <> nil);
  Result := AIterator.GetDataset.IteratorGetKey(AIterator);
end;

function AkvBaseSystem.IteratorGetValue(const AIterator: AkvDatasetIterator): AkvValue;
begin
  Assert(Assigned(AIterator));
  Assert(AIterator.GetDataset <> nil);
  Result := AIterator.GetDataset.IteratorGetValue(AIterator);
end;

function AkvBaseSystem.IteratorGetTimestamp(const AIterator: AkvDatasetIterator): Int64;
begin
  Assert(Assigned(AIterator));
  Assert(AIterator.GetDataset <> nil);
  Result := AIterator.GetDataset.IteratorGetTimestamp(AIterator);
end;

function AkvBaseSystem.RecordExists(const ADataset: AkvDataset; const AKey: String): Boolean;
begin
  Assert(Assigned(ADataset));
  Result := ADataset.RecordExists(AKey);
end;

function AkvBaseSystem.GetRecord(const ADataset: AkvDataset; const AKey: String): AkvValue;
begin
  Assert(Assigned(ADataset));
  Result := ADataset.GetRecord(AKey);
end;

procedure AkvBaseSystem.MakePath(const ADataset: AkvDataset; const AKeyPath: String);
begin
  Assert(Assigned(ADataset));
  ADataset.MakePath(AKeyPath);
end;

procedure AkvBaseSystem.AddRecord(const ADataset: AkvDataset; const AKey: String;
          const AValue: AkvValue);
begin
  Assert(Assigned(ADataset));
  ADataset.AddRecord(AKey, AValue);
end;

procedure AkvBaseSystem.SetRecord(const ADataset: AkvDataset; const AKey: String;
          const AValue: AkvValue);
begin
  Assert(Assigned(ADataset));
  ADataset.SetRecord(AKey, AValue);
end;

procedure AkvBaseSystem.AppendRecord(const ADataset: AkvDataset; const AKey: String;
          const AValue: AkvValue);
begin
  Assert(Assigned(ADataset));
  ADataset.AppendRecord(AKey, AValue);
end;

procedure AkvBaseSystem.DeleteRecord(const ADataset: AkvDataset; const AKey: String);
begin
  Assert(Assigned(ADataset));
  ADataset.DeleteRecord(AKey);
end;



end.

