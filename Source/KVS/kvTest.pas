{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{$INCLUDE kvInclude.inc}

unit kvTest;

interface

procedure Test;

implementation

{$IFDEF DEBUG}
{$IFDEF TEST}
{$DEFINE Profile}
{$ENDIF}
{$ENDIF}

uses
  {$IFDEF POSIX}
  Posix.Unistd,
  {$ENDIF}
  {$IFDEF Profile}
  {$IFDEF MSWINDOWS}
  Windows,
  {$ENDIF}
  {$ENDIF}
  IOUtils,
  SysUtils,
  kvHashList,
  kvHash,
  kvStructures,
  kvValues,
  kvObjects,
  kvScriptNodes,
  kvScriptParser,
  kvScriptSystem;

function BasePath: String;
begin
  Result := TPath.GetHomePath;
  {$IFDEF MSWINDOWS}
  Result := Result + '\temp\';
  {$ENDIF}
  {$IFDEF POSIX}
  Result := Result + '/temp/';
  {$ENDIF}
end;

procedure Test_Structs;
begin
  Assert(KV_SystemFile_HeaderSize = 1024);
  Assert(KV_DatabaseListFile_HeaderSize = 1024);
  Assert(KV_DatabaseListFile_RecordSize = 1024);
  Assert(KV_DatasetListFile_HeaderSize = 1024);
  Assert(KV_DatasetListFile_RecordSize = 1024);
  Assert(KV_HashFile_HeaderSize = 1024);
  Assert(KV_HashFile_RecordSize = 128);
  Assert(KV_BlobFile_HeaderSize = 1024);
  Assert(KV_BlobFile_RecordHeaderSize = 16);
end;

procedure Test_Hash1;
const
  TestN = 10000;
type
  TTestArray = array[0..TestN - 1] of UInt64;
  PTestArray = ^TTestArray;
var
  I, J : Integer;
  P : PTestArray;
  F : UInt64;
  Q : PUInt64;
begin
  New(P);
  try
    for I := 0 to TestN - 1 do
      P^[I] := kvLevel1HashString(IntToStr(I), True);
    for I := 0 to TestN - 2 do
      begin
        F := P^[I];
        Q := @P^[I + 1];
        for J := I + 1 to TestN - 1 do
          begin
            Assert(F <> Q^);
            Inc(Q);
          end;
      end;
  finally
    Dispose(P);
  end;
end;

procedure Test_Hash2;
var
  I : Integer;
  H1, H2 : UInt64;
begin
  // kvLevel1HashString
  Assert(kvLevel1HashString('', True) = kvLevel1HashString('', False));

  Assert(kvLevel1HashString('1', True) = kvLevel1HashString('1', True));
  Assert(kvLevel1HashString('1', False) = kvLevel1HashString('1', False));

  Assert(kvLevel1HashString('a', True) <> kvLevel1HashString('A', True));
  Assert(kvLevel1HashString('a', True) = $CC866A5F640543AC);
  Assert(kvLevel1HashString('A', True) = $1EB43D47A289974);

  Assert(kvLevel1HashString('a', False) = kvLevel1HashString('A', False));
  Assert(kvLevel1HashString('a', False) = $1EB43D47A289974);
  Assert(kvLevel1HashString('A', False) = $1EB43D47A289974);

  Assert(kvLevel1HashString('ab', True) <> kvLevel1HashString('ba', True));

  Assert(kvLevel1HashString('Hello world', True) = $EE34E4B5EF1D9331);
  Assert(kvLevel1HashString('Hello world', False) = $7EDF79A0B38B1ACA);

  // kvLevelNHash
  H1 := $100;
  H2 := $200;
  for I := 1 to 1000000 do
    begin
      H1 := kvLevelNHash(H1);
      H2 := kvLevelNHash(H2);
      Assert(H1 <> $100);
      Assert(H2 <> $200);
      Assert(H1 <> H2);
    end;
end;

procedure Test_HashListHashString;
begin
  Assert(kvhlHashString('a') = $C711B4BF);
  Assert(kvhlHashString('b') = $CCAA8AD3);
  Assert(kvhlHashString('i') = $EADA25DF);
  Assert(kvhlHashString('ab') = $707B03CE);
  Assert(kvhlHashString('abc') = $82DE6531);
  Assert(kvhlHashString(#0#0#0#0) = $6271041F);
  Assert(kvhlHashString('ab') <> kvhlHashString('ba'));
end;

procedure Test_HashList;
var
  A : TkvStringHashList;
  I : Integer;
  It : TkvStringHashListIterator;
begin
  A := TkvStringHashList.Create(True, True, False);
  try
    Assert(A.Count = 0);
    A.Add('a', A);
    Assert(A.Count = 1);
    Assert(A.RequireValue('a') = A);
    for I := 1 to 100000 do
      A.Add(IntToStr(I), A);
    Assert(A.Count = 100001);
    Assert(A.KeyExists('1'));
    A.DeleteKey('1');
    Assert(A.Count = 100000);
    Assert(not A.KeyExists('1'));
    Assert(Assigned(A.IterateFirst(It)));
    for I := 1 to 99999 do
      Assert(Assigned(A.IterateNext(It)));
    Assert(not Assigned(A.IterateNext(It)));
    A.Clear;
    Assert(A.Count = 0);
  finally
    A.Free;
  end;
end;

procedure Test_VarWord32;
var
  Buf : array[0..3] of Byte;
  A : Word32;
begin
  Assert(kvVarWord32EncodedSize(0) = 1);
  Assert(kvVarWord32EncodedSize($7F) = 1);
  Assert(kvVarWord32EncodedSize($80) = 4);

  Assert(kvVarWord32EncodeBuf(1, Buf, 4) = 1);
  Assert(kvVarWord32DecodeBuf(Buf, 4, A) = 1);
  Assert(A = 1);

  Assert(kvVarWord32EncodeBuf($7F, Buf, 4) = 1);
  Assert(kvVarWord32DecodeBuf(Buf, 4, A) = 1);
  Assert(A = $7F);

  Assert(kvVarWord32EncodeBuf($80, Buf, 4) = 4);
  Assert(kvVarWord32DecodeBuf(Buf, 4, A) = 4);
  Assert(A = $80);

  Assert(kvVarWord32EncodeBuf($81, Buf, 4) = 4);
  Assert(kvVarWord32DecodeBuf(Buf, 4, A) = 4);
  Assert(A = $81);

  Assert(kvVarWord32EncodeBuf($12345678, Buf, 4) = 4);
  Assert(kvVarWord32DecodeBuf(Buf, 4, A) = 4);
  Assert(A = $12345678);
end;

procedure Test_System;
var
  Sys : TkvSystem;
  Db : TkvDatabase;
  Ds : TkvDataset;
  VI : TkvIntegerValue;
  VS : TkvStringValue;
  VL : TkvListValue;
  VD : TkvDictionaryValue;
  Va : AkvValue;
  I, J : Integer;
  S, T : String;
  It : TkvDatasetIterator;
  {$IFDEF Profile}{$IFDEF MSWINDOWS}
  T1 : LongWord;
  {$ENDIF}{$ENDIF}
const
  TestN1 = 100000;
begin
  DeleteFile(BasePath + 'testsys.kvsys');
  DeleteFile(BasePath + 'testsys.kvdbl');
  DeleteFile(BasePath + 'testsys._sys.info.kvh');
  DeleteFile(BasePath + 'testsys._sys.info.k.kvbl');
  DeleteFile(BasePath + 'testsys._sys.info.v.kvbl');
  DeleteFile(BasePath + 'testsys.TESTDB.kvdsl');
  DeleteFile(BasePath + 'testsys.TESTDB.testds.kvh');
  DeleteFile(BasePath + 'testsys.TESTDB.testds.k.kvbl');
  DeleteFile(BasePath + 'testsys.TESTDB.testds.v.kvbl');
  DeleteFile(BasePath + 'testsys.TESTDB.testds2.kvh');
  DeleteFile(BasePath + 'testsys.TESTDB.testds2.k.kvbl');
  DeleteFile(BasePath + 'testsys.TESTDB.testds2.v.kvbl');

  VI := TkvIntegerValue.Create;
  VS := TkvStringValue.Create;
  Sys := TkvSystem.Create(BasePath, 'testsys');
  try
    // system create/delete
    Assert(not Sys.Exists);
    Sys.OpenNew;
    try
      Assert(Sys.Exists);
      Sys.Close;
      Assert(Sys.Exists);
    finally
      Sys.Delete;
    end;
    Assert(not Sys.Exists);
    // system create
    Sys.OpenNew('abc');
    Sys.Close;
    // system open
    Assert(Sys.Exists);
    Sys.Open;
    try
      // system attributes
      Assert(Sys.UserDataStr = 'abc');
      Sys.UserDataStr := '123';
      Assert(Sys.UserDataStr = '123');
      // system unique id
      Assert(Sys.AllocateSystemUniqueId = 1);
      Assert(Sys.AllocateSystemUniqueId = 2);
      // create database
      Assert(not Sys.DatabaseExists('TESTDB'));
      Assert(Sys.GetDatabaseCount = 0);
      Db := Sys.CreateDatabase('TESTDB');
      Assert(Assigned(Db));
      Assert(Db.Name = 'TESTDB');
      Assert(Sys.GetDatabaseCount = 1);
      Assert(Sys.DatabaseExists('TESTDB'));
      Assert(Db = Sys.RequireDatabaseByName('TESTDB'));
      // database unique id
      Assert(Sys.AllocateDatabaseUniqueId('TESTDB') = 1);
      Assert(Sys.AllocateDatabaseUniqueId('TESTDB') = 2);
      // create datasets
      Assert(not Sys.DatasetExists('TESTDB', 'testds'));
      Ds := Sys.CreateDataset('TESTDB', 'testds');
      Assert(Assigned(Ds));
      Assert(Sys.DatasetExists('TESTDB', 'testds'));
      Ds := Sys.CreateDataset('TESTDB', 'testds2');
      Assert(Sys.DatasetExists('TESTDB', 'testds2'));
      Assert(Ds <> Sys.RequireDatasetByName('TESTDB', 'testds'));
      Assert(Ds = Sys.RequireDatasetByName('TESTDB', 'testds2'));
      // dataset unique id
      Assert(Sys.AllocateDatasetUniqueId('TESTDB', 'testds') = 1);
      Assert(Sys.AllocateDatasetUniqueId('TESTDB', 'testds') = 2);
      Assert(Sys.AllocateDatasetUniqueId('TESTDB', 'testds2') = 1);
      Assert(Sys.AllocateDatasetUniqueId('TESTDB', 'testds') = 3);
      Assert(Sys.AllocateDatasetUniqueId('TESTDB', 'testds2') = 2);
      // add record
      Assert(not Ds.RecordExists('testkey'));
      VI.Value := 0;
      Ds.AddRecord('testkey', VI);
      Assert(Ds.RecordExists('testkey'));
      // add records
      {$IFDEF Profile}{$IFDEF MSWINDOWS}
      T1 := GetTickCount;
      {$ENDIF}{$ENDIF}
      for I := 1 to TestN1 do
        begin
          VI.Value := I;
          Assert(not Ds.RecordExists(IntToStr(I)));
          Ds.AddRecord(IntToStr(I), VI);
          Assert(Ds.RecordExists(IntToStr(I)));
        end;
      {$IFDEF Profile}{$IFDEF MSWINDOWS}
      T1 := LongWord(GetTickCount - T1);
      Writeln('Add:', T1 / 1000:0:2, 's');
      {$ENDIF}{$ENDIF}
      // records exist
      {$IFDEF Profile}{$IFDEF MSWINDOWS}
      T1 := GetTickCount;
      {$ENDIF}{$ENDIF}
      for I := 1 to TestN1 do
        Assert(Ds.RecordExists(IntToStr(I)));
      {$IFDEF Profile}{$IFDEF MSWINDOWS}
      T1 := LongWord(GetTickCount - T1);
      Writeln('Locate:', T1 / 1000:0:2, 's');
      {$ENDIF}{$ENDIF}
      // get records
      Va := Ds.GetRecord('testkey');
      Assert(Va.TypeId = KV_Value_TypeId_Integer);
      Assert(Va.AsString = '0');
      Va.Free;
      for I := 1 to TestN1 do
        begin
          S := IntToStr(I);
          Va := Ds.GetRecord(S);
          Assert(Va.TypeId = KV_Value_TypeId_Integer);
          Assert(Va.AsString = S);
          Va.Free;
        end;
      // set record
      Va := Ds.GetRecord('102');
      Assert(Assigned(Va));
      Assert(Va.AsString = '102');
      Va.Free;
      VI.Value := 10;
      Ds.SetRecord('102', VI);
      Va := Ds.GetRecord('102');
      Assert(Assigned(Va));
      Assert(Va.AsString = '10');
      Va.Free;
      Assert(Ds.GetRecordAsInteger('102') = 10);
      Assert(Ds.GetRecordAsString('102') = '10');
      // delete record
      S := IntToStr(103);
      Assert(Ds.RecordExists(S));
      Ds.DeleteRecord(S);
      Assert(not Ds.RecordExists(S));
      // delete records
      for I := 10 to 19 do
        begin
          S := IntToStr(I);
          Assert(Ds.RecordExists(S));
          Ds.DeleteRecord(S);
          Assert(not Ds.RecordExists(S));
        end;
      // iterate records
      Ds.DeleteRecord('testkey');
      Ds.DeleteRecord('102');
      Assert(Ds.IterateRecords('', It));
      for I := 1 to TestN1 - 13 do
        begin
          S := Ds.IteratorGetKey(It);
          Va := Ds.IteratorGetValue(It);
          Assert(Va.AsString = S);
          Va.Free;
          Assert(Ds.IterateNextRecord(It));
        end;
      Assert(not Ds.IterateNextRecord(It));
      // long key
      S := '1234567890123456789012345678901234567890';
      VI.Value := 40;
      Ds.AddRecord(S, VI);
      Assert(Ds.RecordExists(S));
      Va := Ds.GetRecord(S);
      Assert(Va.AsString = '40');
      Ds.DeleteRecord(S);
      Assert(not Ds.RecordExists(S));
      Va.Free;
      // long key
      S := '12345678901234567890123456789012345678901234567890123456789012345678901234567890' +
           '12345678901234567890123456789012345678901234567890123456789012345678901234567890' +
           '12345678901234567890123456789012345678901234567890123456789012345678901234567890';
      VI.Value := 50;
      Ds.AddRecord(S, VI);
      Assert(Ds.RecordExists(S));
      Va := Ds.GetRecord(S);
      Assert(Va.AsString = '50');
      Ds.DeleteRecord(S);
      Assert(not Ds.RecordExists(S));
      Va.Free;
      // long key - growing size
      S := '';
      for I := 1 to 2048 do
        begin
          S := S + 'x';
          VI.Value := 50;
          Ds.AddRecord(S, VI);
          Assert(Ds.RecordExists(S));
          Va := Ds.GetRecord(S);
          Assert(Va.AsString = '50');
          Ds.DeleteRecord(S);
          Assert(not Ds.RecordExists(S));
          Va.Free;
        end;
      // long value
      S := '1234567890123456789012345678901234567890';
      VS.Value := S;
      Ds.AddRecord('longv', VS);
      Assert(Ds.RecordExists('longv'));
      Va := Ds.GetRecord('longv');
      Assert(Va.AsString = S);
      Va.Free;
      // long value
      S := '12345678901234567890123456789012345678901234567890123456789012345678901234567890';
      VS.Value := S;
      Ds.SetRecord('longv', VS);
      Va := Ds.GetRecord('longv');
      Assert(Va.AsString = S);
      Va.Free;
      // long value
      S := '12345678901234567890123456789012345678901234567890123456789012345678901234567890' +
           '12345678901234567890123456789012345678901234567890123456789012345678901234567890' +
           '12345678901234567890123456789012345678901234567890123456789012345678901234567890' +
           '12345678901234567890123456789012345678901234567890123456789012345678901234567890' +
           '12345678901234567890123456789012345678901234567890123456789012345678901234567890' +
           '12345678901234567890123456789012345678901234567890123456789012345678901234567890' +
           '12345678901234567890123456789012345678901234567890123456789012345678901234567890' +
           '12345678901234567890123456789012345678901234567890123456789012345678901234567890' +
           '12345678901234567890123456789012345678901234567890123456789012345678901234567890' +
           '12345678901234567890123456789012345678901234567890123456789012345678901234567890';
      VS.Value := S;
      Ds.SetRecord('longv', VS);
      Va := Ds.GetRecord('longv');
      Assert(Va.AsString = S);
      Va.Free;
      // long value - grow and shrink
      S := '';
      for I := 1 to 4096 do
        begin
          VS.Value := S;
          Ds.SetRecord('longv', VS);
          Va := Ds.GetRecord('longv');
          Assert(Va.AsString = S);
          Va.Free;
          S := S + '.';
        end;
      for I := 1 to 4096 do
        begin
          Delete(S, 1, 1);
          VS.Value := S;
          Ds.SetRecord('longv', VS);
          Va := Ds.GetRecord('longv');
          Assert(Va.AsString = S);
          Va.Free;
        end;
      // append short and long
      S := '';
      VS.AsString := '';
      Ds.AddRecord('append', VS);
      for I := 1 to 4096 do
        begin
          VS.AsString := '.';
          Ds.AppendRecord('append', VS);
          Va := Ds.GetRecord('append');
          S := S + '.';
          Assert(Va.AsString = S);
          Va.Free;
      end;
      Ds.DeleteRecord('append');
      // append long and long
      VS.AsString := '';
      Ds.AddRecord('append', VS);
      S := '';
      for I := 1 to 500 do
        S := S + '1234567890';
      VS.AsString := S;
      Ds.AppendRecord('append', VS);
      Va := Ds.GetRecord('append');
      Assert(Va.AsString = S);
      Va.Free;
      T := '';
      for I := 1 to 500 do
        T := T + '123.567890';
      VS.AsString := T;
      Ds.AppendRecord('append', VS);
      Va := Ds.GetRecord('append');
      Assert(Va.AsString = S + T);
      Va.Free;
      Ds.DeleteRecord('append');
      // append list
      VL := TkvListValue.Create;
      Ds.AddRecord('append', VL);
      VL.Add(TkvIntegerValue.Create(1));
      S := '';
      for I := 1 to 500 do
        begin
          Ds.AppendRecord('append', VL);
          if I > 1 then
            S := S + ',';
          S := S + '1';
          Va := Ds.GetRecord('append');
          Assert(Va.AsString = '[' + S + ']');
          Va.Free;
        end;
      Ds.DeleteRecord('append');
      VL.Free;
      // append dictionary
      VD := TkvDictionaryValue.Create;
      Ds.AddRecord('append', VD);
      for I := 1 to 500 do
        begin
          VD.Clear;
          VD.Add(IntToStr(I), TkvIntegerValue.Create(I));
          Ds.AppendRecord('append', VD);
          Va := Ds.GetRecord('append');
          for J := 1 to I do
            begin
              Assert(TkvDictionaryValue(Va).Exists(IntToStr(J)));
              Assert(TkvDictionaryValue(Va).GetValueAsInteger(IntToStr(J)) = J);
            end;
          Va.Free;
        end;
      VD.Free;
      // close database
      Sys.Close;
    finally
      Sys.Delete;
    end;
    Assert(not Sys.Exists);
  finally
    Sys.Free;
    VI.Free;
    VS.Free;
  end;
end;

procedure Test_Parser;
var
  P : TkvScriptParser;

  procedure Parse(const S: String; const Res: String = '');
  var
    V : AkvScriptNode;
    R : String;
    VS : String;
  begin
    V := P.Parse(S);
    try
      if Res = '' then
        R := S
      else
        R := Res;
      VS := V.GetAsString;
      VS := VS.Replace(#13#10, ' ');
      VS := VS.Replace('  ', ' ');
      VS := VS.Trim;
      Assert(VS = R);
    finally
      V.Free;
    end;
  end;

begin
  P := TkvScriptParser.Create;
  try
    Parse('CREATE DATABASE test');
    Parse('CREATE DATABASE "test 123"');

    Parse('CREATE DATASET test:test');
    Parse('CREATE DATASET "test 123":"test 123"');
    Parse('CREATE DATASET test');
    Parse('CREATE DATASET "test 123"');

    Parse('USE test');
    Parse('USE test:test');
    Parse('USE "test 123":"test 123"');

    Parse('INSERT test:test\test 123');
    Parse('INSERT test\test 123');
    Parse('INSERT test 123');
    Parse('INSERT test "test"');

    Parse('DELETE test:test\test');
    Parse('DELETE test\test');
    Parse('DELETE test');
    Parse('DELETE "test 123":"test 123"\"test 123"');

    Parse('UPDATE test:test\test "test 123"');
    Parse('UPDATE "test 123" "test 123"');

    Parse('SELECT test:test\test');
    Parse('SELECT test\test');
    Parse('SELECT test');
    Parse('SELECT "test 123":"test 123"\"test 123"');

    Parse('SELECT 1:1\1');

    Parse('UPDATE 1:1\1 {name:"john",age:10}');
    Parse('UPDATE 1:1\1 {name:"john"}');
    Parse('UPDATE 1:1\1 {}');

    Parse('UPDATE 1:1\1 ["john",10]');
    Parse('UPDATE 1:1\1 ["john"]');
    Parse('UPDATE 1:1\1 []');

    Parse('UPDATE 1:1\1 true');
    Parse('UPDATE 1:1\1 false');

    Parse('SELECT 1:1\1.1');
    Parse('SELECT 1:1\1.1.1');
    Parse('UPDATE 1:1\1.1.1 0');
    Parse('DELETE 1:1\1.1.1');
    Parse('INSERT 1:1\1.@ {a:"test"}');
    Parse('INSERT 1:1\1.c {a:"test"}');

    Parse('SELECT 1:1\1[0]');
    Parse('SELECT 1:1\1.test[0]');

    Parse('SELECT 1:1\1/2/3');
    Parse('SELECT 1\1/2/3');
    Parse('SELECT 1/2/3');

    Parse('UPDATE 1:1\1 1 = 1',
          'UPDATE 1:1\1 (1 = 1)');

    Parse('UPDATE 1:1\1 1 + 1',
          'UPDATE 1:1\1 (1 + 1)');

    Parse('SELECT 1:1\1.1 + 1 + 2',
          '((SELECT 1:1\1.1 + 1) + 2)');

    Parse('SELECT 1:1\1.1 + 1 * 2',
          '(SELECT 1:1\1.1 + (1 * 2))');

    Parse('INSERT 1.addr {city:"ny",state:"ny"}');
    Parse('INSERT 1.@ {addr:{}}');

    Parse('SELECT @db:@ds\@rec.name[1]');

    Parse('CREATE PROCEDURE proc1(@par1,@par2) BEGIN RETURN @par1 + @par2; END',
          'CREATE PROCEDURE proc1(@par1, @par2) BEGIN RETURN (@par1 + @par2); END');

    Parse('SET @a = 1');
    Parse('SET @b = "EVAL @a"');
    Parse('SET @d = SELECT @a.name');

    Parse('IF 1=2 THEN INSERT 1 "A" ELSE INSERT 1 "B"',
          'IF (1 = 2) THEN INSERT 1 "A" ELSE INSERT 1 "B"');

    Parse('INSERT 1 IF 1=1 THEN "A" ELSE "B"',
          'INSERT 1 IF (1 = 1) THEN "A" ELSE "B"');

    Parse('UPDATE 1 SELECT 1 + 1',
          'UPDATE 1 (SELECT 1 + 1)');

    Parse('WHILE @a < 20 BEGIN SET @a = @a + 1; SET @a = @a + 1; END',
          'WHILE (@a < 20) BEGIN SET @a = (@a + 1); SET @a = (@a + 1); END');

    Parse('EVAL (1 + 2) * 3 + 4',
          'EVAL (((1 + 2) * 3) + 4)');

    Parse('INSERT 3 SELECT 1 and SELECT 2',
          'INSERT 3 (SELECT 1 AND SELECT 2)');

    Parse('INSERT 4 not SELECT 1',
          'INSERT 4 NOT (SELECT 1)');
  finally
    P.Free;
  end;
end;

procedure Delete_Script_Database;
begin
  DeleteFile(BasePath + 'testsys.kvsys');
  DeleteFile(BasePath + 'testsys.kvdbl');
  DeleteFile(BasePath + 'testsys._sys.info.kvh');
  DeleteFile(BasePath + 'testsys._sys.info.k.kvbl');
  DeleteFile(BasePath + 'testsys._sys.info.v.kvbl');
  DeleteFile(BasePath + 'testsys.TESTDB.kvdsl');
  DeleteFile(BasePath + 'testsys.TESTDB.testds.kvh');
  DeleteFile(BasePath + 'testsys.TESTDB.testds.k.kvbl');
  DeleteFile(BasePath + 'testsys.TESTDB.testds.v.kvbl');
end;

procedure Check_Script_Result(const Ses: TkvSession; const N: AkvScriptNode; const ValS: String);
var
  V : AkvValue;
begin
  if N is AkvScriptStatement then
    begin
      V := AkvScriptStatement(N).Execute(Ses.ScriptContext);
      if Assigned(V) then
        try
          Assert(V.AsString = ValS);
        finally
          V.Free;
        end
      else
        Assert(ValS = '');
    end
  else
  if N is AkvScriptExpression then
    begin
      V := AkvScriptExpression(N).Evaluate(Ses.ScriptContext);
      if Assigned(V) then
        try
          Assert(V.AsString = ValS);
        finally
          V.Free;
        end
      else
        Assert(ValS = '');
    end
  else
    Assert(ValS = '');
end;

procedure Test_Script_Expressions;
var
  P : TkvScriptParser;
  Sys : TkvSystem;
  MSys : TkvScriptSystem;
  Ses : TkvSession;

  procedure Exec(const S: String; const ValS: String = '');
  var
    N : AkvScriptNode;
  begin
    N := P.Parse(S);
    try
      Check_Script_Result(Ses, N, ValS);
    finally
      N.Free;
    end;
  end;

begin
  Delete_Script_Database;

  P := TkvScriptParser.Create;
  Sys := TkvSystem.Create(BasePath, 'testsys');
  MSys := TkvScriptSystem.Create(Sys);
  try
    MSys.OpenNew;
    Ses := MSys.AddSession;

    Exec('EVAL 1+2', '3');
    Exec('EVAL 1 + 2 * 3 + 4', '11');
    Exec('EVAL (1 + 2) * 3 + 4', '13');
    Exec('EVAL 1 + 2 * (3 + 4)', '15');

    Exec('EVAL INTEGER("3" + "3")', '33');
    Exec('EVAL INTEGER("3") + INTEGER("3")', '6');
    Exec('EVAL STRING(3) + STRING(3)', '33');
    Exec('EVAL FLOAT("1.5") + 1.5', '3');

    Exec('SET @a = BYTE(65) + BYTE(66)');
    Exec('EVAL STRING(@a)', 'AB');
    Exec('EVAL @a = "AB"', 'true');
    Exec('SET @b = BINARY("AB")');
    Exec('EVAL @a = @b', 'true');
    Exec('SET @b = BINARY("A")');
    Exec('EVAL @a > @b', 'true');
    Exec('SET @a = BYTE("0x43")');
    Exec('EVAL @a', 'C');

    Exec('EVAL CHAR(65) + CHAR(66)', 'AB');
    Exec('EVAL CHAR("65") + CHAR("0x42")', 'AB');

    Exec('EVAL LEN("123")', '3');
    Exec('SET @a = [1,2]');
    Exec('EVAL LEN(@a)', '2');

    Exec('EVAL REPLACE("Hello", "l", "m")', 'Hemmo');
    Exec('EVAL REPLACE("Hello", "l", "")', 'Heo');

    Exec('EVAL UPPER("Hello")', 'HELLO');
    Exec('EVAL LOWER("World")', 'world');

    Exec('EVAL TRIM(" Hello world  ")', 'Hello world');

    Exec('EVAL SUBSTRING("Hello world", 2, 3)', 'llo');

    Exec('EVAL INDEXOF("Hell", "Hello world")', '0');
    Exec('EVAL INDEXOF("world", "Hello world")', '6');
    Exec('EVAL INDEXOF("abc", "Hello world")', '-1');

    Exec('EVAL LEFT("Hello", 4) = "Hell"', 'true');
    Exec('EVAL RIGHT("Hello", 4) = "ello"', 'true');

    Exec('EVAL ROUND(1.1)', '1');
    Exec('EVAL ROUND(1.5)', '2');
    Exec('EVAL ROUND(1.5) + ROUND(5.5)', '8');

    Exec('SET @a = GETDATE()');
    Exec('SET @a = GETDATE');

    Exec('EVAL ISNULL(null, "Hello")', 'Hello');
    Exec('SET @a = null');
    Exec('EVAL ISNULL(@a, "Hello")', 'Hello');
    Exec('EVAL @a = null', 'true');
    Exec('EVAL @a <> null', 'false');
    Exec('SET @a = "world"');
    Exec('EVAL ISNULL(@a, "Hello")', 'world');
    Exec('EVAL @a = null', 'false');
    Exec('EVAL @a <> null', 'true');

    Exec('EXEC "EVAL 1+1"', '2');
    Exec('SET @a = 1');
    Exec('EXEC "EVAL @a"', '1');
    Exec('SET @b = "EVAL @a"');
    Exec('EXEC @b', '1');

    Exec('SET @a = [1,2,3]');
    Exec('EVAL @a[1]', '2');
    Exec('EVAL 2 IN @a', 'true');
    Exec('EVAL 4 IN @a', 'false');
    Exec('EVAL "2" IN @a', 'false');
    Exec('EVAL "a" IN @a', 'false');

    Exec('SET @a = [1,[4,5],3]');
    Exec('EVAL @a[1]', '[4,5]');
    Exec('EVAL @a[1][1]', '5');
    Exec('EVAL 1 IN @a', 'true');
    Exec('EVAL 4 IN @a', 'false');

    Exec('SET @a = [1,2] + 3');
    Exec('EVAL @a', '[1,2,3]');

    Exec('SET @a = [1,2] + [3,4]');
    Exec('EVAL @a', '[1,2,3,4]');

    Exec('SET @a = [1,2] + [[3,4]]');
    Exec('EVAL @a', '[1,2,[3,4]]');

    Exec('SET @a = {a:1,b:2,c:3}');
    Exec('EVAL @a.b', '2');

    Exec('SET @a = {a:1,b:{b1:1,b2:2},c:3}');
    Exec('EVAL @a.b', '{b1:1,b2:2}');
    Exec('EVAL @a.b.b2', '2');
    Exec('EVAL "b" in @a', 'true');
    Exec('EVAL "d" in @a', 'false');

    Exec('SET @a = SETOF(["a", "b", "c"])');
    Exec('EVAL @a', 'SETOF(["b","c","a"])');
    Exec('EVAL "a" IN @a', 'true');
    Exec('EVAL "d" IN @a', 'false');
    Exec('SET @a = @a + "d"');
    Exec('EVAL @a', 'SETOF(["b","c","d","a"])');
    Exec('EVAL "a" IN @a', 'true');
    Exec('EVAL "d" IN @a', 'true');
    Exec('SET @a = @a - "d"');
    Exec('EVAL @a', 'SETOF(["b","c","a"])');
    Exec('EVAL "d" IN @a', 'false');

    Exec('SET @a = SETOF("a")');
    Exec('EVAL @a', 'SETOF(["a"])');
    Exec('SET @a = @a + SETOF(["a","b","c"])');
    Exec('EVAL @a', 'SETOF(["b","c","a"])');

    Exec('SET @a = SETOF("a","b","c")');
    Exec('EVAL @a', 'SETOF(["b","c","a"])');

    Exec('SET @a = SETOF()');
    Exec('EVAL @a', 'SETOF([])');

    Ses.Close;
    MSys.Close;

    MSys.Delete;
  finally
    MSys.Free;
    Sys.Free;
    P.Free;
  end;
end;

procedure Test_Script_Database;
var
  P : TkvScriptParser;
  Sys : TkvSystem;
  MSys : TkvScriptSystem;
  Ses : TkvSession;

  procedure Exec(const S: String; const ValS: String = '');
  var
    N : AkvScriptNode;
  begin
    N := P.Parse(S);
    try
      Check_Script_Result(Ses, N, ValS);
    finally
      N.Free;
    end;
  end;

var
  S : String;

begin
  Delete_Script_Database;

  P := TkvScriptParser.Create;
  Sys := TkvSystem.Create(BasePath, 'testsys');
  MSys := TkvScriptSystem.Create(Sys);
  try
    MSys.OpenNew;
    Ses := MSys.AddSession;

    Exec('CREATE DATABASE TESTDB');
    Exec('CREATE DATASET TESTDB:testds');

    Exec('INSERT TESTDB:testds\1 1');
    Exec('SELECT TESTDB:testds\1', '1');
    Exec('UPDATE TESTDB:testds\1 "test"');
    Exec('SELECT TESTDB:testds\1', 'test');
    Exec('EVAL EXISTS TESTDB:testds\1', 'true');
    Exec('DELETE TESTDB:testds\1');

    Exec('USE TESTDB');

    Exec('INSERT testds\1 1');
    Exec('SELECT testds\1', '1');
    Exec('UPDATE testds\1 "test"');
    Exec('SELECT testds\1', 'test');
    Exec('EVAL EXISTS testds\1', 'true');
    Exec('DELETE testds\1');

    Exec('USE TESTDB:testds');

    Exec('SET @a = "TESTDB"');
    Exec('SET @b = "testds"');
    Exec('USE @a:@b');

    Exec('INSERT 1 1');
    Exec('SELECT 1', '1');
    Exec('UPDATE 1 "test"');
    Exec('SELECT 1', 'test');
    Exec('EXISTS 1', 'true');
    Exec('EXISTS 2', 'false');
    Exec('IF NOT EXISTS 2 THEN INSERT 2 123');
    Exec('EXISTS 2', 'true');
    Exec('SELECT 2', '123');
    Exec('DELETE 1');
    Exec('DELETE 2');

    Exec('INSERT 1 {name:"john",age:10}');
    Exec('SELECT 1', '{name:"john",age:10}');
    Exec('UPDATE 1 {name:"john"}');
    Exec('SELECT 1', '{name:"john"}');
    Exec('UPDATE 1 {}');
    Exec('SELECT 1', '{}');
    Exec('DELETE 1');

    Exec('INSERT 1 ["john",10]');
    Exec('SELECT 1', '["john",10]');
    Exec('UPDATE 1 ["john"]');
    Exec('SELECT 1', '["john"]');
    Exec('UPDATE 1 []');
    Exec('SELECT 1', '[]');
    Exec('DELETE 1');

    Exec('INSERT 1 SETOF(["john","peter"])');
    Exec('SELECT 1', 'SETOF(["peter","john"])');
    Exec('UPDATE 1 SETOF()');
    Exec('SELECT 1', 'SETOF([])');
    Exec('DELETE 1');

    Exec('INSERT 1 {name:"john",age:10}');
    Exec('SELECT 1.name', 'john');
    Exec('SELECT 1.age', '10');
    Exec('DELETE 1');

    Exec('INSERT 1 {name:"john",addr:{city:"ny"}}');
    Exec('SELECT 1', '{name:"john",addr:{city:"ny"}}');
    Exec('EXISTS 1', 'true');
    Exec('EXISTS 1.name', 'true');
    Exec('EXISTS 1.names', 'false');
    Exec('SELECT 1.name', 'john');
    Exec('SELECT 1.addr', '{city:"ny"}');
    Exec('SELECT 1.addr.city', 'ny');
    Exec('UPDATE 1.name "peter"');
    Exec('SELECT 1.name', 'peter');
    Exec('UPDATE 1.addr.city "sf"');
    Exec('SELECT 1.addr.city', 'sf');
    Exec('DELETE 1.addr.city');
    Exec('SELECT 1', '{name:"peter",addr:{}}');
    Exec('DELETE 1.addr');
    Exec('SELECT 1', '{name:"peter"}');
    Exec('DELETE 1.name');
    Exec('SELECT 1', '{}');
    Exec('DELETE 1');

    Exec('INSERT 1 {}');
    Exec('SELECT 1', '{}');
    Exec('INSERT 1.@ {name:"john"}');
    Exec('SELECT 1', '{name:"john"}');
    Exec('INSERT 1.@ {addr:{}}');
    Exec('SELECT 1', '{name:"john",addr:{}}');
    Exec('INSERT 1.addr {city:"ny",state:"ny"}');
    Exec('SELECT 1', '{name:"john",addr:{city:"ny",state:"ny"}}');
    Exec('DELETE 1');

    Exec('INSERT 1 [1,2,3]');
    Exec('EXISTS 1', 'true');
    Exec('EXISTS 1[0]', 'true');
    Exec('EXISTS 1[3]', 'false');
    Exec('SELECT 1', '[1,2,3]');
    Exec('UPDATE 1[2] 9');
    Exec('SELECT 1', '[1,2,9]');
    Exec('SELECT 1[2]', '9');
    Exec('DELETE 1[0]');
    Exec('SELECT 1', '[2,9]');
    Exec('INSERT 1[0] 5');
    Exec('SELECT 1', '[5,2,9]');
    Exec('INSERT 1[-1] 12');
    Exec('SELECT 1', '[5,2,9,12]');
    Exec('INSERT 1[4] 13');
    Exec('SELECT 1', '[5,2,9,12,13]');
    Exec('INSERT 1[5] [1,2]');
    Exec('SELECT 1', '[5,2,9,12,13,[1,2]]');
    Exec('DELETE 1[5]');
    Exec('SELECT 1', '[5,2,9,12,13]');
    Exec('DELETE 1[1]');
    Exec('SELECT 1', '[5,9,12,13]');
    Exec('DELETE 1');

    Exec('INSERT 1 true');
    Exec('SELECT 1', 'true');
    Exec('UPDATE 1 false');
    Exec('SELECT 1', 'false');
    Exec('DELETE 1');

    Exec('INSERT 1 BYTE(65)');
    Exec('SELECT 1', 'A');
    Exec('UPDATE 1 BINARY("AB") + BYTE(67)');
    Exec('SELECT 1', 'ABC');
    Exec('DELETE 1');

    Exec('INSERT 1 1');
    Exec('UPDATE 1 SELECT 1 + 1');
    Exec('SELECT 1', '2');
    Exec('DELETE 1');

    Exec('INSERT 1 "test"');
    Exec('UPDATE 1 SELECT 1 + "123"');
    Exec('SELECT 1', 'test123');
    Exec('DELETE 1');

    Exec('INSERT 1 true');
    Exec('INSERT 2 false');
    Exec('INSERT 3 SELECT 1 and SELECT 2');
    Exec('SELECT 3', 'false');
    Exec('INSERT 4 not SELECT 1');
    Exec('SELECT 4', 'false');
    Exec('DELETE 1');
    Exec('DELETE 2');
    Exec('DELETE 3');
    Exec('DELETE 4');

    Exec('INSERT 1 1.7');
    Exec('INSERT 2 1.2');
    Exec('INSERT 3 SELECT 1 + SELECT 2');
    Exec('SELECT 3', '2.9');
    Exec('DELETE 1');
    Exec('DELETE 2');
    Exec('DELETE 3');

    Exec('IF 1=2 THEN INSERT 1 "A" ELSE INSERT 1 "B"');
    Exec('SELECT 1', 'B');
    Exec('DELETE 1');

    Exec('IF 1=1 THEN BEGIN INSERT 1 "A" END ELSE BEGIN INSERT 1 "B" END');
    Exec('SELECT 1', 'A');
    Exec('DELETE 1');

    Exec('INSERT 1 IF 1=1 THEN "A" ELSE "B"');
    Exec('SELECT 1', 'A');
    Exec('DELETE 1');

    Exec('INSERT 1 123');
    Exec('SET @a = SELECT 1');
    Exec('INSERT 2 @a');
    Exec('SELECT 2', '123');
    Exec('SET @a = @a + 1');
    Exec('EVAL @a', '124');
    Exec('EVAL @a+@a', '248');
    Exec('UPDATE 2 @a');
    Exec('SELECT 2', '124');
    Exec('DELETE 1');
    Exec('DELETE 2');

    Exec('INSERT 1 2');
    Exec('INSERT 2 "test"');
    Exec('INSERT 3 {name:"john"}');
    Exec('SET @a = SELECT 1');
    Exec('SET @b = SELECT @a');
    Exec('EVAL @b', 'test');
    Exec('SET @a = 3');
    Exec('SET @d = SELECT @a.name');
    Exec('EVAL @d', 'john');
    Exec('SELECT TESTDB:testds\@a.name', 'john');
    Exec('DELETE 1');
    Exec('DELETE 2');
    Exec('DELETE 3');

    Exec('INSERT 1 [1,2,3,4]');
    Exec('SET @a = 1');
    Exec('SELECT 1[@a]', '2');
    Exec('SET @b = SELECT 1');
    Exec('EVAL @b', '[1,2,3,4]');
    Exec('EVAL @b[1]', '2');
    Exec('SET @b[1] = 99');
    Exec('EVAL @b', '[1,99,3,4]');
    Exec('SET @b[-1] = 5');
    Exec('EVAL @b', '[1,99,3,4,5]');
    Exec('EVAL LEN(@b)', '5');
    Exec('UPDATE 1 @b');
    Exec('SELECT 1', '[1,99,3,4,5]');
    Exec('DELETE 1');

    Exec('INSERT 1 {name:"john",city:"ny"}');
    Exec('SET @b = SELECT 1');
    Exec('EVAL @b', '{name:"john",city:"ny"}');
    Exec('EVAL @b.name', 'john');
    Exec('SET @b.name = "peter"');
    Exec('SET @b.age = 21');
    Exec('EVAL @b', '{name:"peter",city:"ny",age:21}');
    Exec('UPDATE 1 @b');
    Exec('SELECT 1', '{name:"peter",city:"ny",age:21}');
    Exec('DELETE 1');

    Exec('INSERT 1 null');
    Exec('SELECT 1', 'null');
    Exec('UPDATE 1 {name:"john",city:null}');
    Exec('SELECT 1', '{name:"john",city:null}');
    Exec('UPDATE 1.name null');
    Exec('SELECT 1', '{name:null,city:null}');
    Exec('SET @a = null');
    Exec('EVAL @a', 'null');
    Exec('DELETE 1');

    //S := DateTimeToStr(Now);
    //Exec('INSERT 1 DATETIME("' + S + '")');
    // Exec('SELECT 1', S);
    //Exec('DELETE 1');

    Exec('SET @a = 0');
    Exec('WHILE @a < 10 SET @a = @a + 1');
    Exec('EVAL @a', '10');
    Exec('WHILE @a < 20 BEGIN SET @a = @a + 1; SET @a = @a + 1; END');
    Exec('EVAL @a', '20');

    Exec('EVAL LIST_OF_DATABASES', '["TESTDB"]');
    Exec('EVAL LIST_OF_DATASETS("TESTDB")', '["testds"]');

    Exec('INSERT 1 1');
    Exec('INSERT 2 2');
    Exec('INSERT 3 3');
    Exec('INSERT 4 4');
    Exec('ITERATE_RECORDS TESTDB:testds @a');
    Exec('EVAL @a', 'true');
    Exec('EVAL ITERATOR_KEY @a = ITERATOR_VALUE @a', 'true');
    Exec('ITERATE_NEXT @a');
    Exec('EVAL @a', 'true');
    Exec('EVAL ITERATOR_KEY @a = ITERATOR_VALUE @a', 'true');
    Exec('ITERATE_NEXT @a');
    Exec('EVAL @a', 'true');
    Exec('EVAL ITERATOR_KEY @a = ITERATOR_VALUE @a', 'true');
    Exec('ITERATE_NEXT @a');
    Exec('EVAL @a', 'true');
    Exec('EVAL ITERATOR_KEY @a = ITERATOR_VALUE @a', 'true');
    Exec('ITERATE_NEXT @a');
    Exec('EVAL @a', 'false');
    Exec('DELETE 1');
    Exec('DELETE 2');
    Exec('DELETE 3');
    Exec('DELETE 4');

    Exec('INSERT 1 ""');
    Exec('APPEND 1 "Hello"');
    Exec('SELECT 1', 'Hello');
    Exec('APPEND 1 " world"');
    Exec('SELECT 1', 'Hello world');
    Exec('DELETE 1');

    Exec('INSERT 1 "Hi "');
    Exec('APPEND 1 "Hello"');
    Exec('SELECT 1', 'Hi Hello');
    Exec('APPEND 1 " world"');
    Exec('SELECT 1', 'Hi Hello world');
    Exec('DELETE 1');

    Exec('INSERT 1 BINARY("")');
    Exec('APPEND 1 BINARY("Hello")');
    Exec('SELECT 1', 'Hello');
    Exec('APPEND 1 BINARY(" world")');
    Exec('SELECT 1', 'Hello world');
    Exec('DELETE 1');

    Exec('INSERT 1 {a:[1]}');
    Exec('SELECT 1.a', '[1]');
    Exec('APPEND 1.a 2');
    Exec('SELECT 1.a', '[1,2]');
    Exec('APPEND 1.a 3');
    Exec('SELECT 1.a', '[1,2,3]');
    Exec('DELETE 1');

    Exec('INSERT 1 {a:"aa"}');
    Exec('SELECT 1.a', 'aa');
    Exec('APPEND 1.a "bb"');
    Exec('SELECT 1.a', 'aabb');
    Exec('APPEND 1.a 3');
    Exec('SELECT 1.a', 'aabb3');
    Exec('DELETE 1');

    Exec('INSERT 1 []');
    Exec('APPEND 1 [1]');
    Exec('SELECT 1', '[1]');
    Exec('APPEND 1 [2,3]');
    Exec('SELECT 1', '[1,2,3]');
    Exec('APPEND 1 [[1]]');
    Exec('SELECT 1', '[1,2,3,[1]]');
    Exec('DELETE 1');

    Exec('INSERT 1 {}');
    Exec('APPEND 1 {name:"john"}');
    Exec('SELECT 1', '{name:"john"}');
    Exec('APPEND 1 {name2:"john2"}');
    Exec('SELECT 1', '{name:"john",name2:"john2"}');
    Exec('DELETE 1');

    Exec('CREATE PROCEDURE proc1(@par1, @par2) BEGIN RETURN @par1 + @par2 END');
    Exec('EVAL proc1(1, 2) + proc1(5, 6)', '14');

    Exec('CREATE PROCEDURE proc2 BEGIN RETURN 5 END');
    Exec('EVAL proc2', '5');

    Exec('CREATE PROCEDURE TESTDB:proc3(@par1) BEGIN RETURN @par1 + 1; END');
    Exec('USE TESTDB');
    Exec('EVAL proc3(1)', '2');
    Exec('EVAL proc3(2)', '3');

    Exec('USE TESTDB');
    Exec('CREATE PROCEDURE TESTDB:proc4(@par1) BEGIN USE *; RETURN @par1 + 1; END');
    Exec('EVAL proc4(1)', '2');
    Exec('EVAL proc4(2)', '3');

    Exec('EVAL UNIQUE_ID', '1');
    Exec('EVAL UNIQUE_ID', '2');
    Exec('EVAL UNIQUE_ID TESTDB', '1');
    Exec('EVAL UNIQUE_ID TESTDB', '2');
    Exec('EVAL UNIQUE_ID TESTDB:testds', '1');
    Exec('EVAL UNIQUE_ID TESTDB:testds', '2');

    Exec('USE TESTDB:testds');
    Exec('INSERT P1 "Persist"');

    Ses.Close;
    MSys.Close;

    MSys.Open;
    Ses := MSys.AddSession;

    Exec('USE TESTDB:testds');
    Exec('SELECT P1', 'Persist');

    Exec('EVAL UNIQUE_ID', '3');
    Exec('EVAL UNIQUE_ID TESTDB', '3');
    Exec('EVAL UNIQUE_ID TESTDB:testds', '3');

    Exec('USE TESTDB');
    Exec('EVAL proc3(1)', '2');
    Exec('EVAL proc3(2)', '3');

    Exec('EVAL proc4(1)', '2');
    Exec('EVAL proc4(2)', '3');

    Exec('DROP PROCEDURE TESTDB:proc4');

    Exec('USE TESTDB');
    Exec('DROP DATASET testds');

    Exec('USE *');
    Exec('DROP DATABASE TESTDB');

    Ses.Close;
    MSys.Close;

    MSys.Open;
    Ses := MSys.AddSession;

    Exec('CREATE DATABASE TESTDB');
    Exec('CREATE DATASET TESTDB:testds');
    Exec('INSERT TESTDB:testds\1 1');

    Ses.Close;
    MSys.Close;

    MSys.Delete;

    MSys.OpenNew;
    MSys.Close;

  finally
    MSys.Free;
    Sys.Free;
    P.Free;
  end;
end;

procedure Test_Script_Folders;
var
  P : TkvScriptParser;
  Sys : TkvSystem;
  MSys : TkvScriptSystem;
  Ses : TkvSession;

  procedure Exec(const S: String; const ValS: String = '');
  var
    N : AkvScriptNode;
  begin
    N := P.Parse(S);
    try
      Check_Script_Result(Ses, N, ValS);
    finally
      N.Free;
    end;
  end;

begin
  Delete_Script_Database;

  P := TkvScriptParser.Create;
  Sys := TkvSystem.Create(BasePath, 'testsys');
  MSys := TkvScriptSystem.Create(Sys);
  try
    MSys.OpenNew;
    Ses := MSys.AddSession;

    Exec('CREATE DATABASE TESTDB');
    Exec('CREATE DATASET TESTDB:testds');
    Exec('USE TESTDB:testds');

    Exec('INSERT 1/2/3 3');
    Exec('INSERT 1/2/1 1');
    Exec('INSERT 1/2/2 2');
    Exec('SELECT 1/2/1', '1');
    Exec('SELECT 1/2/2', '2');
    Exec('SELECT 1/2/3', '3');
    Exec('SELECT "1/2/3"', '3');
    Exec('SELECT testds\1/2/3', '3');
    Exec('SELECT TESTDB:testds\1/2/3', '3');

    Exec('UPDATE 1/2/3 2');
    Exec('SELECT 1/2/3', '2');
    Exec('SELECT 1/2/3 / 2', '1');

    Exec('EVAL EXISTS 1/2/1', 'true');
    Exec('EVAL EXISTS 1/2/2', 'true');
    Exec('EVAL EXISTS 1/2/3', 'true');
    Exec('EVAL EXISTS 1/2/4', 'false');

    Exec('EVAL EXISTS 1/2', 'true');
    Exec('EVAL EXISTS 1', 'true');

    Exec('DELETE 1/2/3');
    Exec('EVAL EXISTS 1/2/1', 'true');
    Exec('EVAL EXISTS 1/2/2', 'true');
    Exec('EVAL EXISTS 1/2/3', 'false');
    Exec('EVAL EXISTS 1/2', 'true');
    Exec('EVAL EXISTS 1', 'true');

    Exec('DELETE 1');
    Exec('EVAL EXISTS 1/2/1', 'false');
    Exec('EVAL EXISTS 1/2/2', 'false');
    Exec('EVAL EXISTS 1/2/3', 'false');
    Exec('EVAL EXISTS 1/2', 'false');
    Exec('EVAL EXISTS 1', 'false');

    Exec('INSERT 1/2/1 1');
    Exec('INSERT 1/2/2 2');
    Exec('INSERT 1/2/3 3');
    Exec('DELETE 1/2');
    Exec('EVAL EXISTS 1/2/1', 'false');
    Exec('EVAL EXISTS 1/2/2', 'false');
    Exec('EVAL EXISTS 1/2/3', 'false');
    Exec('EVAL EXISTS 1/2', 'false');
    Exec('EVAL EXISTS 1', 'true');

    Exec('INSERT 1/1/1 1');
    Exec('INSERT 1/2 2');
    Exec('INSERT 1/3/3/3 3');

    Exec('ITERATE_RECORDS TESTDB:testds @a');
    Exec('EVAL @a', 'true');
    Exec('SET @b1 = ITERATOR_KEY @a');
    Exec('EVAL (@b1 = "1/1/1") OR (@b1 = "1/2") OR (@b1 = "1/3/3/3")', 'true');
    Exec('EVAL INTEGER(RIGHT(@b1, 1)) = ITERATOR_VALUE @a', 'true');
    Exec('ITERATE_NEXT @a');
    Exec('EVAL @a', 'true');
    Exec('SET @b2 = ITERATOR_KEY @a');
    Exec('EVAL @b2 <> @b1', 'true');
    Exec('EVAL (@b1 = "1/1/1") OR (@b1 = "1/2") OR (@b1 = "1/3/3/3")', 'true');
    Exec('EVAL INTEGER(RIGHT(@b2, 1)) = ITERATOR_VALUE @a', 'true');
    Exec('ITERATE_NEXT @a');
    Exec('EVAL @a', 'true');
    Exec('SET @b3 = ITERATOR_KEY @a');
    Exec('EVAL @b3 <> @b2', 'true');
    Exec('EVAL (@b1 = "1/1/1") OR (@b1 = "1/2") OR (@b1 = "1/3/3/3")', 'true');
    Exec('EVAL INTEGER(RIGHT(@b3, 1)) = ITERATOR_VALUE @a', 'true');
    Exec('ITERATE_NEXT @a');
    Exec('EVAL @a', 'false');

    Exec('ITERATE_RECORDS TESTDB:testds\1/1 @a');
    Exec('EVAL @a', 'true');
    Exec('EVAL ITERATOR_KEY @a', '1/1/1');
    Exec('EVAL ITERATOR_VALUE @a', '1');
    Exec('ITERATE_NEXT @a');
    Exec('EVAL @a', 'false');

    Exec('ITERATE_RECORDS TESTDB:testds\1/3 @a');
    Exec('EVAL @a', 'true');
    Exec('EVAL ITERATOR_KEY @a', '1/3/3/3');
    Exec('EVAL ITERATOR_VALUE @a', '3');
    Exec('ITERATE_NEXT @a');
    Exec('EVAL @a', 'false');

    Exec('ITERATE_RECORDS TESTDB:testds\1/3/3 @a');
    Exec('EVAL @a', 'true');
    Exec('EVAL ITERATOR_KEY @a', '1/3/3/3');
    Exec('EVAL ITERATOR_VALUE @a', '3');
    Exec('ITERATE_NEXT @a');
    Exec('EVAL @a', 'false');

    Exec('DELETE 1');
    Exec('ITERATE_RECORDS TESTDB:testds @a');
    Exec('EVAL @a', 'false');

    Exec('MKPATH 1/2/');
    Exec('EVAL EXISTS 1', 'true');
    Exec('EVAL EXISTS 1/2', 'true');
    Exec('EVAL EXISTS 1/2/3', 'false');
    Exec('DELETE 1');

    Exec('MKPATH 1/2');
    Exec('EVAL EXISTS 1', 'true');
    Exec('EVAL EXISTS 1/2', 'true');
    Exec('EVAL EXISTS 1/2/3', 'false');
    Exec('DELETE 1');

    Exec('MKPATH 1');
    Exec('MKPATH 1/2');
    Exec('EVAL EXISTS 1', 'true');
    Exec('EVAL EXISTS 1/2', 'true');
    Exec('EVAL EXISTS 1/2/3', 'false');

    Exec('INSERT 1/2/3 3');
    Exec('INSERT 1/2/1 1');
    Exec('INSERT 1/2/2 2');

    Exec('SELECT 1/2/3', '3');
    Exec('SELECT 1/2', '{2:2,3:3,1:1}');
    Exec('SELECT 1', '{2:{2:2,3:3,1:1}}');
    Exec('SELECT 1.2', '{2:2,3:3,1:1}');
    Exec('SELECT 1.2.3', '3');

    Exec('SELECT /', '{1:{2:{2:2,3:3,1:1}}}');

    Exec('SELECT /.1', '{2:{2:2,3:3,1:1}}');
    Exec('SELECT /.1.2', '{2:2,3:3,1:1}');
    Exec('SELECT /.1.2.3', '3');

    Exec('DELETE 1');

    Ses.Close;
    MSys.Close;

    MSys.Delete;
  finally
    MSys.Free;
    Sys.Free;
    P.Free;
  end;
end;

procedure Test;
begin
  Test_HashListHashString;
  Test_HashList;
  Test_Hash1;
  Test_Hash2;
  Test_VarWord32;
  Test_Structs;
  Test_System;
  Test_Parser;
  Test_Script_Expressions;
  Test_Script_Database;
  Test_Script_Folders;
end;

end.

