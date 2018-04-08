{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/10  0.01  Initial development (use, create, insert, delete, update) }
{ 2018/02/12  0.02  Identifier tokens }
{ 2018/02/14  0.03  Field references }
{ 2018/02/17  0.04  Binary operators, If statement, If expression }
{ 2018/02/18  0.05  Identifiers, Set statement, Exists expression, Identifier references }
{ 2018/02/19  0.06  Iterators }
{ 2018/02/23  0.07  Stored procedure }
{ 2018/02/26  0.08  UniqueId }
{ 2018/02/28  0.09  Exec statement }
{ 2018/03/03  0.10  Extend record iterator to use path }
{ 2018/03/04  0.11  In operator }
{ 2018/03/05  0.12  Append statement }
{ 2018/04/08  0.13  LIST_OF_KEYS expression }

{$INCLUDE kvInclude.inc}

unit kvScriptParser;

interface

uses
  SysUtils,
  kvScriptNodes;



type
  EkvScriptParser = class(Exception);

  TkvScriptToken = (
      stNone,
      stEndOfBuf,

      stInvalidChar,
      stWhitespace,
      stColon,
      stBackslash,
      stDot,
      stOpenCurly,
      stCloseCurly,
      stComma,
      stSemicolon,
      stOpenBlock,
      stCloseBlock,
      stOpenParenthesis,
      stCloseParenthesis,

      stLessThan,
      stGreaterThan,
      stEqual,
      stNotEqual,
      stLessOrEqual,
      stGreaterOrEqual,

      stAsterisk,
      stDivide,
      stPlus,
      stMinus,

      stIdentifier,
      stQuotedString,
      stInteger,
      stFloat,
      stKeyString,

      stUSE,
      stCREATE,
      stDROP,
      stDATABASE,
      stDATASET,

      stPROCEDURE,
      stRETURN,

      stINSERT,
      stDELETE,
      stSELECT,
      stUPDATE,
      stAPPEND,
      stEXISTS,
      stMKPATH,
      stLIST_OF_KEYS,
      stRECURSE,

      stSET,

      stEVAL,

      stEXEC,

      stUNIQUE_ID,
      stLIST_OF_DATABASES,
      stLIST_OF_DATASETS,

      stITERATE_RECORDS,
      stITERATE_NEXT,
      stITERATOR_KEY,
      stITERATOR_VALUE,

      stNULL,
      stTRUE,
      stFALSE,

      stIN,

      stAND,
      stOR,
      stXOR,
      stNOT,

      stIF,
      stTHEN,
      stELSE,

      stBEGIN,
      stEND,

      stWHILE
    );

  TkvScriptParser = class
  private
    FBuf             : Pointer;
    FBufLen          : Integer;
    FBufStrRef       : String;
    FBufPos          : Integer;
    FToken           : TkvScriptToken;
    FTokenIdentifier : String;
    FTokenString     : String;
    FTokenInt        : Int64;
    FTokenFloat      : Double;

    function  SkipWhitespace: Boolean;
    function  GetIdentifierToken: TkvScriptToken;
    function  GetQuotedStringToken: TkvScriptToken;
    function  GetNumberToken: TkvScriptToken;
    function  GetForcedKeyToken: TkvScriptToken;
    function  GetNextComparisonToken: TkvScriptToken;
    function  GetNextToken: TkvScriptToken;
    function  GetNextTokenAsKey: TkvScriptToken;

    function  GetKeyToken: String;
    procedure ExpectToken(const T: TkvScriptToken; const MsgS: String);
    function  ExpectIdentifier: String;
    function  SkipToken(const T: TkvScriptToken): Boolean;

    function  ParseRecordRef: TkvScriptRecordReference;
    function  ParseFieldRef: TkvScriptRecordAndFieldReference;
    procedure ParseDictionaryKeyValue(var Key: String; var Value: AkvScriptExpression);
    function  ParseDictionaryValue: AkvScriptValue;
    function  ParseListValue: AkvScriptValue;
    function  ParseIfExpression: AkvScriptExpression;
    function  ParseIdentifierRef(const ForSet: Boolean): AkvScriptIdentifierReference;
    function  ParseListOfDatabasesExpression: AkvScriptExpression;
    function  ParseListOfDatasetsExpression: AkvScriptExpression;
    function  ParseIteratorKeyExpression: AkvScriptExpression;
    function  ParseIteratorValueExpression: AkvScriptExpression;
    function  ParseListOfKeysExpression: AkvScriptExpression;
    function  ParseUniqueIdExpression: AkvScriptExpression;
    function  ParseValue: AkvScriptValue;
    function  ParseFactor: AkvScriptExpression;
    function  ParseTerm: AkvScriptExpression;
    function  ParseExpression: AkvScriptExpression;
    function  ParseUseStatement: TkvScriptUseStatement;
    function  ParseCreateDatabaseStatement: AkvScriptStatement;
    function  ParseCreateDatasetStatement: AkvScriptStatement;
    function  ParseCreateProcedureStatement: AkvScriptStatement;
    function  ParseCreateStatement: AkvScriptStatement;
    function  ParseDropDatabaseStatement: AkvScriptStatement;
    function  ParseDropDatasetStatement: AkvScriptStatement;
    function  ParseDropProcedureStatement: AkvScriptStatement;
    function  ParseDropStatement: AkvScriptStatement;
    function  ParseInsertStatement: TkvScriptInsertStatement;
    function  ParseSelectExpression: AkvScriptExpression;
    function  ParseExistsExpression: AkvScriptExpression;
    function  ParseDeleteStatement: TkvScriptDeleteStatement;
    function  ParseUpdateStatement: TkvScriptUpdateStatement;
    function  ParseAppendStatement: AkvScriptStatement;
    function  ParseMakePathStatement: AkvScriptStatement;
    function  ParseIfStatement: TkvScriptIfStatement;
    function  ParseBlockStatement: TkvScriptBlockStatement;
    function  ParseSetStatement: TkvScriptSetStatement;
    function  ParseEvalStatement: TkvScriptEvalStatement;
    function  ParseWhileStatement: TkvScriptWhileStatement;
    function  ParseIterateRecordsStatement: AkvScriptStatement;
    function  ParseIterateNextStatement: AkvScriptStatement;
    function  ParseReturnStatement: AkvScriptStatement;
    function  ParseExecStatement: AkvScriptStatement;
    function  ParseStatement: AkvScriptStatement;
    function  ParseCommand: AkvScriptNode;

  public
    function  Parse(const S: String): AkvScriptNode;
  end;



implementation

uses
  Character;



{ TkvScriptParser }

const
  kvScriptKeywordCount = 40;
  kvScriptKeyword : array[0..kvScriptKeywordCount - 1] of String = (
      'CREATE',
      'DROP',
      'DATABASE',
      'DATASET',
      'PROCEDURE',
      'RETURN',
      'USE',
      'INSERT',
      'DELETE',
      'UPDATE',
      'APPEND',
      'SELECT',
      'EXISTS',
      'MKPATH',
      'LIST_OF_KEYS',
      'RECURSE',
      'SET',
      'EVAL',
      'EXEC',
      'UNIQUE_ID',
      'LIST_OF_DATABASES',
      'LIST_OF_DATASETS',
      'ITERATE_RECORDS',
      'ITERATE_NEXT',
      'ITERATOR_KEY',
      'ITERATOR_VALUE',
      'NULL',
      'TRUE',
      'FALSE',
      'IN',
      'AND',
      'OR',
      'XOR',
      'NOT',
      'IF',
      'THEN',
      'ELSE',
      'BEGIN',
      'END',
      'WHILE'
    );
  kvScriptKeywordTokens : array[0..kvScriptKeywordCount - 1] of TkvScriptToken = (
      stCREATE,
      stDROP,
      stDATABASE,
      stDATASET,
      stPROCEDURE,
      stRETURN,
      stUSE,
      stINSERT,
      stDELETE,
      stUPDATE,
      stAPPEND,
      stSELECT,
      stEXISTS,
      stMKPATH,
      stLIST_OF_KEYS,
      stRECURSE,
      stSET,
      stEVAL,
      stEXEC,
      stUNIQUE_ID,
      stLIST_OF_DATABASES,
      stLIST_OF_DATASETS,
      stITERATE_RECORDS,
      stITERATE_NEXT,
      stITERATOR_KEY,
      stITERATOR_VALUE,
      stNULL,
      stTRUE,
      stFALSE,
      stIN,
      stAND,
      stOR,
      stXOR,
      stNOT,
      stIF,
      stTHEN,
      stELSE,
      stBEGIN,
      stEND,
      stWHILE
    );

function BufMatchKeyword(const Buf: PChar; const BufLen: Integer; const Keyword: String): Boolean;
var
  L, I : Integer;
  P : PChar;
  C : Char;
begin
  L := Length(Keyword);
  if L <> BufLen then
    Result := False
  else
    begin
      P := Buf;
      for I := 1 to L do
        begin
          C := P^;
          C := UpCase(C);
          if Keyword[I] <> C then
            begin
              Result := False;
              exit;
            end;
          Inc(P);
        end;
      Result := True;
    end;
end;

function BufGetKeywordToken(const Buf: PChar; const BufLen: Integer): TkvScriptToken;
var
  I : Integer;
  K : String;
begin
  for I := 0 to kvScriptKeywordCount - 1 do
    begin
      K := kvScriptKeyword[I];
      if BufMatchKeyword(Buf, BufLen, K) then
        begin
          Result := kvScriptKeywordTokens[I];
          exit;
        end;
    end;
  Result := stNone;
end;

function IsIdentifierStartChar(const C: Char): Boolean;
begin
  if Ord(C) > $FF then
    Result := C.IsLetter
  else
    case C of
      'A'..'Z',
      'a'..'z',
      '_',
      '@' : Result := True;
    else
      Result := False;
    end;
end;

function IsIdentifierChar(const C: Char): Boolean;
begin
  if Ord(C) > $FF then
    Result := C.IsLetter or C.IsDigit
  else
    case C of
      'A'..'Z',
      'a'..'z',
      '_',
      '@',
      '0'..'9' : Result := True;
    else
      Result := False;
    end;
end;

function IsWhitespaceChar(const C: Char): Boolean;
begin
  if Ord(C) > $FF then
    Result := C.IsWhiteSpace
  else
    Result := Ord(C) <= 32;
end;

function TkvScriptParser.SkipWhitespace: Boolean;
var
  P : PChar;
  N : Integer;
  C : WideChar;
  R : Boolean;
begin
  N := FBufPos;
  P := FBuf;
  Inc(P, N);
  C := P^;
  R := False;
  while IsWhitespaceChar(C) do
    begin
      R := True;
      Inc(FBufPos);
      if FBufPos >= FBufLen then
        break;
      Inc(P);
      C := P^;
    end;
  Result := R;
end;

function TkvScriptParser.GetIdentifierToken: TkvScriptToken;
var
  P : PChar;
  N : Integer;
  Q : PChar;
  C : WideChar;
  T : TkvScriptToken;
  IdenLen : Integer;
begin
  N := FBufPos;
  P := FBuf;
  Inc(P, N);
  Q := P;
  C := P^;
  Assert(IsIdentifierStartChar(C));
  IdenLen := 0;
  repeat
    Inc(IdenLen);
    Inc(FBufPos);
    if FBufPos >= FBufLen then
      break;
    Inc(P);
    C := P^;
  until not IsIdentifierChar(C);
  T := BufGetKeywordToken(Q, IdenLen);
  if T = stNone then
    begin
      T := stIdentifier;
      SetLength(FTokenIdentifier, IdenLen);
      Move(Q^, PChar(FTokenIdentifier)^, IdenLen * SizeOf(Char));
    end;
  Result := T;
end;

function TkvScriptParser.GetQuotedStringToken: TkvScriptToken;
var
  P : PChar;
  N : Integer;
  C : WideChar;
  S : TStringBuilder;
  R : Boolean;
begin
  N := FBufPos;
  P := FBuf;
  Inc(P, N);
  C := P^;
  Assert(C = '"');
  S := TStringBuilder.Create;
  try
    R := False;
    repeat
       Inc(N);
       if N >= FBufLen then
         raise EkvScriptParser.Create('" expected');
       Inc(P);
       C := P^;
       if C = '"' then
         begin
           Inc(N);
           if N >= FBufLen then
             R := True
           else
             begin
               Inc(P);
               C := P^;
               if C = '"' then
                 S.Append('"')
               else
                 R := True;
             end;
         end
       else
         S.Append(C);
    until R;
    FBufPos := N;
    FTokenString := S.ToString;
  finally
    S.Free;
  end;
  Result := stQuotedString;
end;

function IsNumberChar(const C: Char): Boolean;
begin
  Result := ((C >= '0') and (C <= '9')) or
            (C = '.');
end;

function TkvScriptParser.GetNumberToken: TkvScriptToken;
var
  P : PChar;
  N : Integer;
  C : WideChar;
  Q : PChar;
  L : Integer;
  S : String;
begin
  N := FBufPos;
  P := FBuf;
  Inc(P, N);
  Q := P;
  L := 0;
  repeat
    Inc(L);
    Inc(FBufPos);
    if FBufPos >= FBufLen then
      break;
    Inc(P);
    C := P^;
  until not IsNumberChar(C);
  SetLength(S, L);
  Move(Q^, PChar(S)^, L * SizeOf(Char));
  if TryStrToInt64(S, FTokenInt) then
    Result := stInteger
  else
  if TryStrToFloat(S, FTokenFloat) then
    Result := stFloat
  else
    raise EkvScriptParser.CreateFmt('Invalid number: %s', [S])
end;

function TkvScriptParser.GetForcedKeyToken: TkvScriptToken;
var
  P : PChar;
  N : Integer;
  C : WideChar;
  Q : PChar;
  L : Integer;
  S : String;
begin
  N := FBufPos;
  P := FBuf;
  Inc(P, N);
  Q := P;
  L := 0;
  repeat
    Inc(L);
    Inc(FBufPos);
    if FBufPos >= FBufLen then
      break;
    Inc(P);
    C := P^;
  until (C = ':') or (C = '.') or (C = '\') or
        (C = '}') or (C = ']') or (C = ';') or (C = '*') or
        (C = '?') or (C = ',') or (C = '<') or (C = '[') or
        (C = '{') or (C = '>') or (C = '(') or (C = ')') or
        C.IsWhiteSpace or
        (Ord(C) <= 32);
  SetLength(S, L);
  Move(Q^, PChar(S)^, L * SizeOf(Char));
  FTokenIdentifier := S;
  Result := stIdentifier;
end;

function TkvScriptParser.GetNextComparisonToken: TkvScriptToken;
var
  P : PChar;
  N : Integer;
  C, D : WideChar;
  T : TkvScriptToken;
begin
  N := FBufPos;
  P := FBuf;
  Inc(P, N);
  C := P^;
  Assert((C = '<') or (C = '>') or (C = '='));
  Inc(FBufPos);

  Inc(N);
  Inc(P);
  if N < FBufLen then
    D := P^
  else
    D := #0;
  if (D = '>') or (D = '=') then
    Inc(FBufPos)
  else
    D := #0;
  if (C = '<') and (D = #0) then
    T := stLessThan
  else
  if (C = '>') and (D = #0) then
    T := stGreaterThan
  else
  if (C = '=') and (D = #0) then
    T := stEqual
  else
  if (C = '<') and (D = '>') then
    T := stNotEqual
  else
  if (C = '<') and (D = '=') then
    T := stLessOrEqual
  else
  if (C = '>') and (D = '=') then
    T := stGreaterOrEqual
  else
    T := stNone;
  Result := T;
end;

function TkvScriptParser.GetNextToken: TkvScriptToken;
var
  P : PChar;
  N : Integer;
  C : WideChar;
  T : TkvScriptToken;
begin
  repeat
    T := stNone;
    N := FBufPos;
    if N >= FBufLen then
      begin
        T := stEndOfBuf;
        break;
      end;
    P := FBuf;
    Inc(P, N);
    C := P^;
    if IsWhitespaceChar(C) or (Ord(C) < 32) then
      T := stWhitespace
    else
      case C of
        '*' : T := stAsterisk;
        '/' : T := stDivide;
        '+' : T := stPlus;
        '-' : T := stMinus;
        ':' : T := stColon;
        '\' : T := stBackslash;
        '.' : T := stDot;
        '{' : T := stOpenCurly;
        '}' : T := stCloseCurly;
        ',' : T := stComma;
        ';' : T := stSemicolon;
        '[' : T := stOpenBlock;
        ']' : T := stCloseBlock;
        '(' : T := stOpenParenthesis;
        ')' : T := stCloseParenthesis;
      end;
    if T <> stNone then
      Inc(FBufPos)
    else
      case C of
        '"'           : T := GetQuotedStringToken;
        '0'..'9'      : T := GetNumberToken;
        '<', '>', '=' : T := GetNextComparisonToken;
      else
        if IsIdentifierStartChar(C) then
          T := GetIdentifierToken
        else
          begin
            T := stInvalidChar;
            Inc(FBufPos);
          end;
      end;
  until T <> stWhitespace;
  FToken := T;
  Result := T;
end;

function TkvScriptParser.GetNextTokenAsKey: TkvScriptToken;
var
  P : PChar;
  N : Integer;
  C : WideChar;
  T : TkvScriptToken;
begin
  T := stNone;
  N := FBufPos;
  if N >= FBufLen then
    begin
      Result := stEndOfBuf;
      exit;
    end;
  P := FBuf;
  Inc(P, N);
  C := P^;
  if IsWhitespaceChar(C) then
    T := stWhitespace
  else
    case C of
      '*' : T := stAsterisk;
      '+' : T := stPlus;
      '-' : T := stMinus;
      ':' : T := stColon;
      '\' : T := stBackslash;
      '.' : T := stDot;
      '{' : T := stOpenCurly;
      '}' : T := stCloseCurly;
      ',' : T := stComma;
      ';' : T := stSemicolon;
      '[' : T := stOpenBlock;
      ']' : T := stCloseBlock;
      '(' : T := stOpenParenthesis;
      ')' : T := stCloseParenthesis;
    end;
  if T <> stNone then
    Inc(FBufPos)
  else
    case C of
      '"'           : T := GetQuotedStringToken;
      '<', '>', '=' : T := GetNextComparisonToken;
    else
      T := GetForcedKeyToken;
    end;
  FToken := T;
  Result := T;
end;

function TkvScriptParser.GetKeyToken: String;
begin
  case FToken of
    stIdentifier   : Result := FTokenIdentifier;
    stQuotedString : Result := FTokenString;
  else
    raise EkvScriptParser.Create('Identifier expected');
  end;
end;

procedure TkvScriptParser.ExpectToken(const T: TkvScriptToken; const MsgS: String);
begin
  if FToken <> T then
    raise EkvScriptParser.Create(Msgs);
  GetNextToken;
end;

function TkvScriptParser.ExpectIdentifier: String;
begin
  if FToken <> stIdentifier then
    raise EkvScriptParser.Create('Identifier expected');
  Result := FTokenIdentifier;
  GetNextToken;
end;

function TkvScriptParser.SkipToken(const T: TkvScriptToken): Boolean;
begin
  if FToken = T then
    begin
      GetNextToken;
      Result := True;
    end
  else
    Result := False;
end;

function TkvScriptParser.ParseRecordRef: TkvScriptRecordReference;
var
  Key : String;
  KeyDatabase : String;
  KeyDataset : String;
  KeyRec : String;
begin
  Key := GetKeyToken;
  GetNextTokenAsKey;
  if FToken = stColon then
    begin
      GetNextTokenAsKey;
      KeyDatabase := Key;
      KeyDataset := GetKeyToken;
      GetNextTokenAsKey;
      if FToken <> stBackslash then
        raise EkvScriptParser.Create('\ expected');
      GetNextTokenAsKey;
      KeyRec := GetKeyToken;
      GetNextTokenAsKey;
    end
  else
  if FToken = stBackslash then
    begin
      GetNextTokenAsKey;
      KeyDatabase := '';
      KeyDataset := Key;
      KeyRec := GetKeyToken;
      GetNextTokenAsKey;
    end
  else
    begin
      KeyDatabase := '';
      KeyDataset := '';
      KeyRec := Key;
    end;
  Result := TkvScriptRecordReference.Create(KeyDatabase, KeyDataset, KeyRec);
end;

function TkvScriptParser.ParseFieldRef: TkvScriptRecordAndFieldReference;
var
  RecRef : TkvScriptRecordReference;
  Key : String;
  FieldRef : AkvScriptFieldReference;
  R : Boolean;
  ListIdx : AkvScriptExpression;
begin
  RecRef := ParseRecordRef;
  FieldRef := nil;
  repeat
    R := False;
    if FToken = stDot then
      begin
        GetNextTokenAsKey;
        Key := GetKeyToken;
        GetNextTokenAsKey;
        FieldRef := TkvScriptFieldNameFieldReference.Create(FieldRef, Key);
        R := True;
      end else
    if FToken = stOpenBlock then
      begin
        GetNextToken;
        ListIdx := ParseExpression;
        if FToken <> stCloseBlock then
          raise EkvScriptParser.Create('] expected');
        GetNextTokenAsKey;
        FieldRef := TkvScriptListIndexFieldReference.Create(FieldRef, ListIdx);
        R := True;
      end;
  until not R;
  if FToken = stWhitespace then
    GetNextToken;
  Result := TkvScriptRecordAndFieldReference.Create(RecRef, FieldRef);
end;

procedure TkvScriptParser.ParseDictionaryKeyValue(var Key: String; var Value: AkvScriptExpression);
begin
  Key := GetKeyToken;
  GetNextToken;

  ExpectToken(stColon, ': expected');

  Value := ParseExpression;
end;

function TkvScriptParser.ParseDictionaryValue: AkvScriptValue;
var
  Key : String;
  Value : AkvScriptExpression;
  List : TkvScriptDictionaryKeyValueArray;
  L, I : Integer;
begin
  Assert(FToken = stOpenCurly);
  SkipWhitespace;
  GetNextTokenAsKey;
  L := 0;
  try
    while FToken <> stCloseCurly do
      begin
        if FToken = stEndOfBuf then
          raise EkvScriptParser.Create('} expected');
        ParseDictionaryKeyValue(Key, Value);
        Inc(L);
        SetLength(List, L);
        List[L - 1].Key := Key;
        List[L - 1].Value := Value;
        if FToken = stComma then
          begin
            SkipWhitespace;
            GetNextTokenAsKey;
            if FToken = stCloseCurly then
              raise EkvScriptParser.Create('Key expected')
          end;
      end;
    GetNextToken;
  except
    for I := Length(List) - 1 downto 0 do
      List[I].Value.Free;
    raise;
  end;
  Result := TkvScriptDictionaryValue.Create(List);
end;

function TkvScriptParser.ParseListValue: AkvScriptValue;
var
  Value : AkvScriptExpression;
  List : TkvScriptListValueArray;
  L, I : Integer;
begin
  Assert(FToken = stOpenBlock);
  GetNextToken;
  L := 0;
  try
    while FToken <> stCloseBlock do
      begin
        if FToken = stEndOfBuf then
          raise EkvScriptParser.Create('] expected');
        Value := ParseExpression;
        Inc(L);
        SetLength(List, L);
        List[L - 1] := Value;
        if FToken = stComma then
          begin
            GetNextToken;
            if FToken = stCloseBlock then
              raise EkvScriptParser.Create('Value expected')
          end;
      end;
    GetNextToken;
  except
    for I := Length(List) - 1 downto 0 do
      List[I].Free;
    raise;
  end;
  Result := TkvScriptListValue.Create(List);
end;

function TkvScriptParser.ParseIfExpression: AkvScriptExpression;
var
  Cond : AkvScriptExpression;
  TrSt : AkvScriptExpression;
  FaSt : AkvScriptExpression;
begin
  Assert(FToken = stIF);
  GetNextToken;

  Cond := ParseExpression;
  ExpectToken(stTHEN, 'THEN expected');

  TrSt := ParseExpression;
  ExpectToken(stELSE, 'ELSE expected');

  FaSt := ParseExpression;

  Result := TkvScriptIfExpression.Create(Cond, TrSt, FaSt);
end;

function TkvScriptParser.ParseIdentifierRef(const ForSet: Boolean): AkvScriptIdentifierReference;
var
  Key : String;
  IdenRef : AkvScriptIdentifierReference;
  R : Boolean;
  ListIdx : AkvScriptExpression;
  L, I : Integer;
  ParamExpr : TkvScriptExpressionArray;
begin
  IdenRef := nil;
  try
    repeat
      R := False;
      if FToken = stDot then
        begin
          GetNextTokenAsKey;
          Key := GetKeyToken;
          GetNextTokenAsKey;
          IdenRef := TkvScriptFieldNameIdentifierReference.Create(IdenRef, Key);
          R := True;
        end else
      if FToken = stOpenBlock then
        begin
          GetNextToken;
          ListIdx := ParseExpression;
          if FToken <> stCloseBlock then
            raise EkvScriptParser.Create('] expected');
          GetNextTokenAsKey;
          IdenRef := TkvScriptListIndexIdentifierReference.Create(IdenRef, ListIdx);
          R := True;
        end else
      if FToken = stOpenParenthesis then
        begin
          if ForSet then
            raise EkvScriptParser.Create('Function call not allowed with SET');
          GetNextToken;
          L := 0;
          try
            while FToken <> stCloseParenthesis do
              begin
                Inc(L);
                SetLength(ParamExpr, L);
                ParamExpr[L - 1] := nil;
                ParamExpr[L - 1] := ParseExpression;
                if FToken = stComma then
                  GetNextToken
                else
                if FToken <> stCloseParenthesis then
                  raise EkvScriptParser.Create(') expected');
              end;
            GetNextToken;
          except
            for I := Length(ParamExpr) - 1 downto 0 do
              ParamExpr[I].Free;
            raise;
          end;
          IdenRef := TkvScriptFunctionCallIdentifierReference.Create(
              IdenRef, ParamExpr);
          R := True;
        end;
    until not R;
    if FToken = stWhitespace then
      GetNextToken;
  except
    IdenRef.Free;
    raise;
  end;
  Result := IdenRef;
end;

function TkvScriptParser.ParseListOfDatabasesExpression: AkvScriptExpression;
begin
  Assert(FToken = stLIST_OF_DATABASES);
  GetNextToken;
  Result := TkvScriptListOfDatabasesExpression.Create;
end;

function TkvScriptParser.ParseListOfDatasetsExpression: AkvScriptExpression;
var
  NameE : AkvScriptExpression;
begin
  Assert(FToken = stLIST_OF_DATASETS);
  GetNextToken;
  ExpectToken(stOpenParenthesis, '( expected');
  NameE := ParseExpression;
  try
    ExpectToken(stCloseParenthesis, ') expected');
  except
    NameE.Free;
    raise;
  end;
  Result := TkvScriptListOfDatasetsExpression.Create(NameE);
end;

function TkvScriptParser.ParseIteratorKeyExpression: AkvScriptExpression;
var
  Iden : String;
begin
  Assert(FToken = stITERATOR_KEY);
  GetNextToken;
  Iden := ExpectIdentifier;
  Result := TkvScriptIteratorKeyExpression.Create(Iden);
end;

function TkvScriptParser.ParseIteratorValueExpression: AkvScriptExpression;
var
  Iden : String;
begin
  Assert(FToken = stITERATOR_VALUE);
  GetNextToken;
  Iden := ExpectIdentifier;
  Result := TkvScriptIteratorValueExpression.Create(Iden);
end;

function TkvScriptParser.ParseListOfKeysExpression: AkvScriptExpression;
var
  RecRef : TkvScriptRecordReference;
  Recurse : Boolean;
begin
  Assert(FToken = stLIST_OF_KEYS);
  SkipWhitespace;
  GetNextTokenAsKey;
  RecRef := ParseRecordRef;
  if FToken = stWhitespace then
    GetNextToken;
  if FToken = stRECURSE then
    begin
      Recurse := True;
      GetNextToken;
    end
  else
    Recurse := False;
  Result := TkvScriptListOfKeysExpression.Create(RecRef, Recurse);
end;

function TkvScriptParser.ParseUniqueIdExpression: AkvScriptExpression;
var
  DbName : String;
  DsName : String;
begin
  Assert(FToken = stUNIQUE_ID);
  SkipWhitespace;
  GetNextTokenAsKey;

  if (FToken <> stIdentifier) and
     (FToken <> stQuotedString) then
    begin
      Result := TkvScriptUniqueIdExpression.Create('', '');
      exit;
    end;

  DbName := GetKeyToken;
  GetNextToken;

  if FToken = stColon then
    begin
      GetNextTokenAsKey;
      DsName := GetKeyToken;
      GetNextToken;
    end;

  Result := TkvScriptUniqueIdExpression.Create(DbName, DsName);
end;

function TkvScriptParser.ParseValue: AkvScriptValue;
var
  T : TkvScriptToken;
begin
  case FToken of
    stPlus, stMinus :
      begin
        T := FToken;
        GetNextToken;
        case FToken of
          stInteger :
            Result := TkvScriptIntegerValue.Create(FTokenInt);
          stFloat :
            Result := TkvScriptFloatValue.Create(FTokenFloat);
        else
          raise EkvScriptParser.Create('Number expected');
        end;
        if T = stMinus then
          Result := TkvScriptNegateValue.Create(Result);
      end;
    stQuotedString :
      Result := TkvScriptStringValue.Create(FTokenString);
    stInteger :
      Result := TkvScriptIntegerValue.Create(FTokenInt);
    stFloat :
      Result := TkvScriptFloatValue.Create(FTokenFloat);
    stNULL :
      Result := TkvScriptNullValue.Create;
    stTRUE,
    stFALSE :
      Result := TkvScriptBooleanValue.Create(FToken = stTRUE);
    stOpenCurly :
      Result := ParseDictionaryValue;
    stOpenBlock :
      Result := ParseListValue;
  else
    Result := nil;
  end;
  if FToken in [stQuotedString, stInteger, stFloat, stNULL, stTRUE, stFALSE] then
    GetNextToken;
end;

function TkvScriptParser.ParseFactor: AkvScriptExpression;
var
  T : TkvScriptToken;
  Ex : AkvScriptExpression;
  RiEx : AkvScriptExpression;
  Iden : String;
  IdenRef : AkvScriptIdentifierReference;
begin
  Ex := ParseValue;
  if not Assigned(Ex) then
    case FToken of
      stOpenParenthesis :
        begin
          GetNextToken;
          Ex := ParseExpression;
          ExpectToken(stCloseParenthesis, ') expected');
        end;
      stSELECT     : Ex := ParseSelectExpression;
      stIF         : Ex := ParseIfExpression;
      stIdentifier :
        begin
          Iden := FTokenIdentifier;
          GetNextToken;
          IdenRef := ParseIdentifierRef(False);
          Ex := TkvScriptIdentifierExpression.Create(Iden, IdenRef);
        end;
      stNOT :
        begin
          GetNextToken;
          Ex := ParseExpression;
          Ex := TkvScriptNOTOperator.Create(Ex);
        end;
      stEXISTS :
        Ex := ParseExistsExpression;
      stUNIQUE_ID :
        Ex := ParseUniqueIdExpression;
      stLIST_OF_DATABASES :
        Ex := ParseListOfDatabasesExpression;
      stLIST_OF_DATASETS :
        Ex := ParseListOfDatasetsExpression;
      stITERATOR_KEY :
        Ex := ParseIteratorKeyExpression;
      stITERATOR_VALUE :
        Ex := ParseIteratorValueExpression;
      stLIST_OF_KEYS :
        Ex := ParseListOfKeysExpression;
    end;
  if not Assigned(Ex) then
    raise EkvScriptParser.Create('Expression expected');
  while FToken in [stAND, stAsterisk, stDivide] do
    begin
      T := FToken;
      GetNextToken;
      RiEx := ParseFactor;
      case T of
        stAND      : Ex := TkvScriptANDOperator.Create(Ex, RiEx);
        stAsterisk : Ex := TkvScriptMultiplyOperator.Create(Ex, RiEx);
        stDivide   : Ex := TkvScriptDivideOperator.Create(Ex, RiEx);
      end;
    end;
  Result := Ex;
end;

function TokenToCompareOp(const T: TkvScriptToken): TkvScriptCompareOperatorType;
begin
  case T of
    stLessThan       : Result := scotLessThan;
    stGreaterThan    : Result := scotGreaterThan;
    stEqual          : Result := scotEqual;
    stNotEqual       : Result := scotNotEqual;
    stLessOrEqual    : Result := scotLessOrEqualThan;
    stGreaterOrEqual : Result := scotGreaterOrEqualThan;
  else
    raise EkvScriptParser.Create('Invalid token');
  end;
end;

function TkvScriptParser.ParseTerm: AkvScriptExpression;
var
  T : TkvScriptToken;
  Ex : AkvScriptExpression;
  RiEx : AkvScriptExpression;
begin
  Ex := ParseFactor;
  while FToken in [stOR, stXOR,
                   stLessThan, stGreaterThan, stEqual, stNotEqual,
                   stLessOrEqual, stGreaterOrEqual,
                   stPlus, stMinus,
                   stIN] do
    begin
      T := FToken;
      GetNextToken;
      RiEx := ParseFactor;
      case T of
        stOR  : Ex := TkvScriptOROperator.Create(Ex, RiEx);
        stXOR : Ex := TkvScriptXOROperator.Create(Ex, RiEx);
        stLessThan,
        stGreaterThan,
        stEqual,
        stNotEqual,
        stLessOrEqual,
        stGreaterOrEqual : Ex := TkvScriptCompareOperator.Create(TokenToCompareOp(T), Ex, RiEx);
        stPlus : Ex := TkvScriptPlusOperator.Create(Ex, RiEx);
        stMinus : Ex := TkvScriptMinusOperator.Create(Ex, RiEx);
        stIN : Ex := TkvScriptInOperator.Create(Ex, RiEx);
      end;
    end;
  Result := Ex;
end;

function TkvScriptParser.ParseExpression: AkvScriptExpression;
begin
  Result := ParseTerm;
end;

function TkvScriptParser.ParseUseStatement: TkvScriptUseStatement;
var
  DbName : String;
  DsName : String;
begin
  Assert(FToken = stUSE);
  SkipWhitespace;
  GetNextTokenAsKey;

  if SkipToken(stAsterisk) then
    begin
      Result := TkvScriptUseStatement.Create('', '');
      exit;
    end;

  DbName := GetKeyToken;
  GetNextToken;

  if FToken = stColon then
    begin
      GetNextTokenAsKey;

      DsName := GetKeyToken;
      GetNextToken;
    end;

  Result := TkvScriptUseStatement.Create(DbName, DsName);
end;

function TkvScriptParser.ParseCreateDatabaseStatement: AkvScriptStatement;
var
  DbName : String;
begin
  Assert(FToken = stDATABASE);
  SkipWhitespace;
  GetNextTokenAsKey;

  DbName := GetKeyToken;
  GetNextToken;

  Result := TkvScriptCreateDatabaseStatement.Create(DbName);
end;

function TkvScriptParser.ParseCreateDatasetStatement: AkvScriptStatement;
var
  Iden : String;
  DbName : String;
  DsName : String;
begin
  Assert(FToken = stDATASET);
  SkipWhitespace;
  GetNextTokenAsKey;

  Iden := GetKeyToken;
  GetNextToken;

  if FToken = stColon then
    begin
      GetNextTokenAsKey;

      DbName := Iden;
      DsName := GetKeyToken;
      GetNextToken;
    end
  else
    DsName := Iden;

  Result := TkvScriptCreateDatasetStatement.Create(DbName, DsName);
end;

function TkvScriptParser.ParseCreateProcedureStatement: AkvScriptStatement;
var
  IdenN : String;
  DbName : String;
  ProcName : String;
  ParamCount : Integer;
  ParamNames : TkvScriptCreateProcedureParamNameArray;
  Statement : AkvScriptStatement;
  Iden : String;
begin
  Assert(FToken = stPROCEDURE);
  GetNextToken;

  IdenN := ExpectIdentifier;
  if FToken = stColon then
    begin
      DbName := IdenN;
      GetNextToken;
      ProcName := ExpectIdentifier;
    end
  else
    ProcName := IdenN;

  ParamCount := 0;
  if FToken = stOpenParenthesis then
    begin
      GetNextToken;
      while FToken <> stCloseParenthesis do
        begin
          Iden := ExpectIdentifier;

          Inc(ParamCount);
          SetLength(ParamNames, ParamCount);
          ParamNames[ParamCount - 1] := Iden;

          if FToken = stComma then
            GetNextToken
          else
          if FToken <> stCloseParenthesis then
            raise EkvScriptParser.Create(') expected');
        end;
      GetNextToken;
    end;

  if FToken <> stBEGIN then
    raise EkvScriptParser.Create('BEGIN expected');
  Statement := ParseBlockStatement;

  Result := TkvScriptCreateProcedureStatement.Create(DbName, ProcName, ParamNames, Statement);
end;

function TkvScriptParser.ParseCreateStatement: AkvScriptStatement;
begin
  Assert(FToken = stCREATE);
  GetNextToken;

  case FToken of
    stDATABASE  : Result := ParseCreateDatabaseStatement;
    stDATASET   : Result := ParseCreateDatasetStatement;
    stPROCEDURE : Result := ParseCreateProcedureStatement;
  else
    raise EkvScriptParser.Create('Unexpected token');
  end;
end;

function TkvScriptParser.ParseDropDatabaseStatement: AkvScriptStatement;
var
  DbName : String;
begin
  Assert(FToken = stDATABASE);
  SkipWhitespace;
  GetNextTokenAsKey;

  DbName := GetKeyToken;
  GetNextToken;

  Result := TkvScriptDropDatabaseStatement.Create(DbName);
end;

function TkvScriptParser.ParseDropDatasetStatement: AkvScriptStatement;
var
  Iden : String;
  DbName : String;
  DsName : String;
begin
  Assert(FToken = stDATASET);
  SkipWhitespace;
  GetNextTokenAsKey;

  Iden := GetKeyToken;
  GetNextToken;

  if FToken = stColon then
    begin
      GetNextTokenAsKey;

      DbName := Iden;
      DsName := GetKeyToken;
      GetNextToken;
    end
  else
    DsName := Iden;

  Result := TkvScriptDropDatasetStatement.Create(DbName, DsName);
end;

function TkvScriptParser.ParseDropProcedureStatement: AkvScriptStatement;
var
  DbName : String;
  ProcName : String;
begin
  Assert(FToken = stPROCEDURE);
  SkipWhitespace;
  GetNextTokenAsKey;

  DbName := GetKeyToken;
  GetNextToken;

  if FToken <> stColon then
    raise EkvScriptParser.Create(': expected');
  GetNextTokenAsKey;

  ProcName := GetKeyToken;
  GetNextToken;

  Result := TkvScriptDropProcedureStatement.Create(DbName, ProcName);
end;

function TkvScriptParser.ParseDropStatement: AkvScriptStatement;
begin
  Assert(FToken = stDROP);
  GetNextToken;

  case FToken of
    stDATABASE  : Result := ParseDropDatabaseStatement;
    stDATASET   : Result := ParseDropDatasetStatement;
    stPROCEDURE : Result := ParseDropProcedureStatement;
  else
    raise EkvScriptParser.Create('Unexpected token');
  end;
end;

function TkvScriptParser.ParseInsertStatement: TkvScriptInsertStatement;
var
  FieldRef : TkvScriptRecordAndFieldReference;
  Val : AkvScriptExpression;
begin
  Assert(FToken = stINSERT);
  SkipWhitespace;
  GetNextTokenAsKey;

  FieldRef := ParseFieldRef;
  Val := ParseExpression;

  Result := TkvScriptInsertStatement.Create(FieldRef, Val);
end;

function TkvScriptParser.ParseExistsExpression: AkvScriptExpression;
var
  FieldRef : TkvScriptRecordAndFieldReference;
begin
  Assert(FToken = stEXISTS);
  SkipWhitespace;
  GetNextTokenAsKey;

  FieldRef := ParseFieldRef;

  Result := TkvScriptExistsExpression.Create(FieldRef);
end;

function TkvScriptParser.ParseSelectExpression: AkvScriptExpression;
var
  FieldRef : TkvScriptRecordAndFieldReference;
begin
  Assert(FToken = stSELECT);
  SkipWhitespace;
  GetNextTokenAsKey;

  FieldRef := ParseFieldRef;

  Result := TkvScriptSelectExpression.Create(FieldRef);
end;

function TkvScriptParser.ParseDeleteStatement: TkvScriptDeleteStatement;
var
  FieldRef : TkvScriptRecordAndFieldReference;
begin
  Assert(FToken = stDELETE);
  SkipWhitespace;
  GetNextTokenAsKey;

  FieldRef := ParseFieldRef;

  Result := TkvScriptDeleteStatement.Create(FieldRef);
end;

function TkvScriptParser.ParseUpdateStatement: TkvScriptUpdateStatement;
var
  FieldRef : TkvScriptRecordAndFieldReference;
  Val : AkvScriptExpression;
begin
  Assert(FToken = stUPDATE);
  SkipWhitespace;
  GetNextTokenAsKey;

  FieldRef := ParseFieldRef;
  Val := ParseExpression;

  Result := TkvScriptUpdateStatement.Create(FieldRef, Val);
end;

function TkvScriptParser.ParseAppendStatement: AkvScriptStatement;
var
  FieldRef : TkvScriptRecordAndFieldReference;
  Val : AkvScriptExpression;
begin
  Assert(FToken = stAPPEND);
  SkipWhitespace;
  GetNextTokenAsKey;

  FieldRef := ParseFieldRef;
  Val := ParseExpression;

  Result := TkvScriptAppendStatement.Create(FieldRef, Val);
end;

function TkvScriptParser.ParseMakePathStatement: AkvScriptStatement;
var
  RecRef : TkvScriptRecordReference;
begin
  Assert(FToken = stMKPATH);
  SkipWhitespace;
  GetNextTokenAsKey;

  RecRef := ParseRecordRef;

  Result := TkvScriptMakePathStatement.Create(RecRef);
end;

function TkvScriptParser.ParseIfStatement: TkvScriptIfStatement;
var
  Cond : AkvScriptExpression;
  TrSt : AkvScriptNode;
  FaSt : AkvScriptNode;
begin
  Assert(FToken = stIF);
  GetNextToken;

  Cond := ParseExpression;
  ExpectToken(stTHEN, 'THEN expected');

  TrSt := ParseCommand;
  if SkipToken(stELSE) then
    FaSt := ParseCommand
  else
    FaSt := nil;

  Result := TkvScriptIfStatement.Create(Cond, TrSt, FaSt);
end;

function TkvScriptParser.ParseBlockStatement: TkvScriptBlockStatement;
var
  L : TkvScriptNodeArray;
  N : Integer;
  C : AkvScriptNode;
begin
  Assert(FToken = stBEGIN);
  GetNextToken;

  N := 0;
  while (FToken <> stEND) and (FToken <> stEndOfBuf) do
    begin
      C := ParseCommand;
      Inc(N);
      SetLength(L, N);
      L[N - 1] := C;
      while SkipToken(stSemicolon) do ;
    end;
  ExpectToken(stEND, 'END expected');

  Result := TkvScriptBlockStatement.Create(L);
end;

function TkvScriptParser.ParseSetStatement: TkvScriptSetStatement;
var
  Iden : String;
  IdenRef : AkvScriptIdentifierReference;
  Val : AkvScriptExpression;
begin
  Assert(FToken = stSET);
  GetNextToken;

  if (FToken <> stIdentifier) or (FTokenIdentifier = '') then
    raise EkvScriptParser.Create('Identifier expected');
  Iden := FTokenIdentifier;
  GetNextToken;

  IdenRef := ParseIdentifierRef(True);

  ExpectToken(stEqual, '= expected');

  Val := ParseExpression;

  Result := TkvScriptSetStatement.Create(Iden, IdenRef, Val);
end;

function TkvScriptParser.ParseEvalStatement: TkvScriptEvalStatement;
var
  Val : AkvScriptExpression;
begin
  Assert(FToken = stEVAL);
  GetNextToken;

  Val := ParseExpression;

  Result := TkvScriptEvalStatement.Create(Val);
end;

function TkvScriptParser.ParseWhileStatement: TkvScriptWhileStatement;
var
  Cond : AkvScriptExpression;
  Stmt : AkvScriptStatement;
begin
  Assert(FToken = stWHILE);
  GetNextToken;

  Cond := ParseExpression;
  Stmt := ParseStatement;

  Result := TkvScriptWhileStatement.Create(Cond, Stmt);
end;

function TkvScriptParser.ParseIterateRecordsStatement: AkvScriptStatement;
var
  KeyDatabase : String;
  KeyDataset : String;
  KeyRec : String;
  KeyIdentifier : String;
begin
  Assert(FToken = stITERATE_RECORDS);
  SkipWhitespace;
  GetNextTokenAsKey;

  KeyDatabase := GetKeyToken;
  GetNextTokenAsKey;
  if FToken <> stColon then
    raise EkvScriptParser.Create(': expected');
  GetNextTokenAsKey;
  KeyDataset := GetKeyToken;
  GetNextTokenAsKey;
  if FToken = stBackslash then
    begin
      GetNextTokenAsKey;
      KeyRec := GetKeyToken;
      GetNextTokenAsKey;
    end;

  SkipWhitespace;
  GetNextToken;
  KeyIdentifier := ExpectIdentifier;

  Result := TkvScriptIterateStatement.Create(KeyDatabase, KeyDataset, KeyRec,
      KeyIdentifier);
end;

function TkvScriptParser.ParseIterateNextStatement: AkvScriptStatement;
var
  Iden : String;
begin
  Assert(FToken = stITERATE_NEXT);
  GetNextToken;

  Iden := ExpectIdentifier;

  Result := TkvScriptIterateNextStatement.Create(Iden);
end;

function TkvScriptParser.ParseReturnStatement: AkvScriptStatement;
var
  Expr : AkvScriptExpression;
begin
  Assert(FToken = stRETURN);
  GetNextToken;

  Expr := ParseExpression;

  Result := TkvScriptReturnStatement.Create(Expr);
end;

function TkvScriptParser.ParseExecStatement: AkvScriptStatement;
var
  Ex : AkvScriptExpression;
begin
  Assert(FToken = stEXEC);
  GetNextToken;

  Ex := ParseExpression;
  Result := TkvScriptExecStatement.Create(Ex);
end;

function TkvScriptParser.ParseStatement: AkvScriptStatement;
begin
  case FToken of
    stUSE             : Result := ParseUseStatement;
    stCREATE          : Result := ParseCreateStatement;
    stDROP            : Result := ParseDropStatement;
    stINSERT          : Result := ParseInsertStatement;
    stDELETE          : Result := ParseDeleteStatement;
    stUPDATE          : Result := ParseUpdateStatement;
    stAPPEND          : Result := ParseAppendStatement;
    stMKPATH          : Result := ParseMakePathStatement;
    stIF              : Result := ParseIfStatement;
    stBEGIN           : Result := ParseBlockStatement;
    stSET             : Result := ParseSetStatement;
    stEVAL            : Result := ParseEvalStatement;
    stWHILE           : Result := ParseWhileStatement;
    stITERATE_RECORDS : Result := ParseIterateRecordsStatement;
    stITERATE_NEXT    : Result := ParseIterateNextStatement;
    stRETURN          : Result := ParseReturnStatement;
    stEXEC            : Result := ParseExecStatement;
  else
    Result := nil;
  end;
end;

function TkvScriptParser.ParseCommand: AkvScriptNode;
begin
  case FToken of
    stEndOfBuf    : Result := nil;
    stInvalidChar : raise EkvScriptParser.Create('Invalid character');
    stSELECT      : Result := ParseExpression;
    stEXISTS      : Result := ParseExistsExpression;
  else
    begin
      Result := ParseStatement;
      if not Assigned(Result) then
        raise EkvScriptParser.Create('Unexpected token');
    end;
  end;
end;

function TkvScriptParser.Parse(const S: String): AkvScriptNode;
begin
  if S = '' then
    begin
      Result := nil;
      exit;
    end;
  FBufStrRef := S;
  FBuf := PChar(FBufStrRef);
  FBufLen := Length(S);
  FBufPos := 0;
  FToken := stNone;
  GetNextToken;
  Result := ParseCommand;
end;



end.

