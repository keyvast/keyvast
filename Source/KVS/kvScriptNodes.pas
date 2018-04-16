{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/10  0.01  Initial development }
{ 2018/02/14  0.02  Field references }
{ 2018/02/17  0.03  Binary operators }
{ 2018/02/17  0.04  Binary operators, If statement, If expression }
{ 2018/02/23  0.05  Stored procedures }
{ 2018/03/01  0.06  Push/Pop used database and dataset on procedure call }
{ 2018/03/12  0.07  Improved locking, reduced value duplication }

{$INCLUDE kvInclude.inc}

unit kvScriptNodes;

interface

uses
  SysUtils,
  kvHashList,
  kvObjects,
  kvValues,
  kvScriptContext,
  kvScriptFunctions;



type
  EkvScriptNode = class(Exception);

  { Node }

  AkvScriptNode = class
  public
    function Duplicate: AkvScriptNode; virtual; abstract;
    function GetAsString: String; virtual; abstract;
  end;
  TkvScriptNodeArray = array of AkvScriptNode;

  { Statement }

  AkvScriptStatement = class(AkvScriptNode)
  public
    function DuplicateStatement: AkvScriptStatement;
    function Execute(const Context: TkvScriptContext): AkvValue; virtual; abstract;
  end;
  TkvScriptStatementArray = array of AkvScriptStatement;

  { Expression }

  AkvScriptExpression = class(AkvScriptNode)
  public
    function DuplicateExpression: AkvScriptExpression;
    function Evaluate(const Context: TkvScriptContext): AkvValue; virtual; abstract;
  end;
  TkvScriptExpressionArray = array of AkvScriptExpression;

  { Value }

  AkvScriptValue = class(AkvScriptExpression)
  protected
    function  GetAsValue: AkvValue; virtual; abstract;
  public
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Block statement }

  TkvScriptBlockStatement = class(AkvScriptStatement)
  private
    FList : TkvScriptNodeArray;

  public
    constructor Create(const List: TkvScriptNodeArray);

    function Duplicate: AkvScriptNode; override;
    function GetAsString: String; override;
    function Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { If statement }

  TkvScriptIfStatement = class(AkvScriptStatement)
  private
    FCondition : AkvScriptExpression;
    FTrueStatement : AkvScriptNode;
    FFalseStatement : AkvScriptNode;

  public
    constructor Create(const Condition: AkvScriptExpression;
                const TrueStatement: AkvScriptNode;
                const FalseStatement: AkvScriptNode);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { If expression }

  TkvScriptIfExpression = class(AkvScriptExpression)
  private
    FCondition : AkvScriptExpression;
    FTrueExpr  : AkvScriptExpression;
    FFalseExpr : AkvScriptExpression;

  public
    constructor Create(const Condition: AkvScriptExpression;
                const TrueExpr: AkvScriptExpression;
                const FalseExpr: AkvScriptExpression);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Identifier reference }

  AkvScriptIdentifierReference = class(AkvScriptNode)
  private
    FIdentifierRef : AkvScriptIdentifierReference;

  public
    constructor Create(const IdentifierRef: AkvScriptIdentifierReference);
    destructor Destroy; override;

    function  GetValue(const Context: TkvScriptContext; const V: TObject; out Owner: Boolean): TObject; virtual; abstract;
    procedure SetValue(const Context: TkvScriptContext; const V: AkvValue; const Val: AkvValue); virtual; abstract;
  end;

  { Field name identifier reference }

  TkvScriptFieldNameIdentifierReference = class(AkvScriptIdentifierReference)
  private
    FFieldName : String;

  public
    constructor Create(const IdentifierRef: AkvScriptIdentifierReference; const FieldName: String);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  GetValue(const Context: TkvScriptContext; const V: TObject; out Owner: Boolean): TObject; override;
    procedure SetValue(const Context: TkvScriptContext; const V: AkvValue; const Value: AkvValue); override;
  end;

  { List index identifier reference }

  TkvScriptListIndexIdentifierReference = class(AkvScriptIdentifierReference)
  private
    FListIndex : AkvScriptExpression;

  public
    constructor Create(const IdentifierRef: AkvScriptIdentifierReference; const ListIndex: AkvScriptExpression);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  GetValue(const Context: TkvScriptContext; const V: TObject; out Owner: Boolean): TObject; override;
    procedure SetValue(const Context: TkvScriptContext; const V: AkvValue; const Value: AkvValue); override;
  end;

  { Function call identifier reference }

  TkvScriptFunctionCallIdentifierReference = class(AkvScriptIdentifierReference)
  private
    FParamExpressions : TkvScriptExpressionArray;

  public
    constructor Create(const IdentifierRef: AkvScriptIdentifierReference;
                const ParamExpressions: TkvScriptExpressionArray);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  GetValue(const Context: TkvScriptContext; const V: TObject; out Owner: Boolean): TObject; override;
    procedure SetValue(const Context: TkvScriptContext; const V: AkvValue; const Value: AkvValue); override;
  end;

  { Identifier expression }

  TkvScriptIdentifierExpression = class(AkvScriptExpression)
  private
    FIdentifier : String;
    FIdentifierRef : AkvScriptIdentifierReference;

  public
    constructor Create(const Identifier: String; const IdentiferRef: AkvScriptIdentifierReference);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Set statement }

  TkvScriptSetStatement = class(AkvScriptStatement)
  private
    FIdentifier    : String;
    FIdentifierRef : AkvScriptIdentifierReference;
    FValue         : AkvScriptExpression;

  public
    constructor Create(
                const Identifier: String;
                const IdentifierRef: AkvScriptIdentifierReference;
                const Value: AkvScriptExpression);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Negate value }

  TkvScriptNegateValue = class(AkvScriptValue)
  private
    FValue : AkvScriptValue;

  protected
    function  GetAsValue: AkvValue; override;

  public
    constructor Create(const Value: AkvScriptValue);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { String value }

  TkvScriptStringValue = class(AkvScriptValue)
  private
    FValue : String;

  public
    constructor Create(const Value: String);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  GetAsValue: AkvValue; override;
  end;

  { Integer value }

  TkvScriptIntegerValue = class(AkvScriptValue)
  private
    FValue : Int64;

  public
    constructor Create(const Value: Int64);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  GetAsValue: AkvValue; override;
  end;

  { Float value }

  TkvScriptFloatValue = class(AkvScriptValue)
  private
    FValue : Double;

  public
    constructor Create(const Value: Double);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  GetAsValue: AkvValue; override;
  end;

  { Boolean value }

  TkvScriptBooleanValue = class(AkvScriptValue)
  private
    FValue : Boolean;

  public
    constructor Create(const Value: Boolean);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  GetAsValue: AkvValue; override;
  end;

  { DateTime value }

  TkvScriptDateTimeValue = class(AkvScriptValue)
  private
    FValue : TDateTime;

  public
    constructor Create(const Value: TDateTime);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  GetAsValue: AkvValue; override;
  end;

  { Null value }

  TkvScriptNullValue = class(AkvScriptValue)
  private
  public
    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  GetAsValue: AkvValue; override;
  end;

  { Dictionary value }

  TkvScriptDictionaryKeyValue = record
    Key   : String;
    Value : AkvScriptExpression;
  end;
  TkvScriptDictionaryKeyValueArray = array of TkvScriptDictionaryKeyValue;
  TkvScriptDictionaryValue = class(AkvScriptValue)
  private
    FValue : TkvScriptDictionaryKeyValueArray;
  public
    constructor Create(const Value: TkvScriptDictionaryKeyValueArray);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  GetAsValue: AkvValue; override;
  end;

  { List value }

  TkvScriptListValueArray = array of AkvScriptExpression;
  TkvScriptListValue = class(AkvScriptValue)
  private
    FValue : TkvScriptListValueArray;

  public
    constructor Create(const Value: TkvScriptListValueArray);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  GetAsValue: AkvValue; override;
  end;

  { Record reference }

  TkvScriptRecordReference = class(AkvScriptNode)
  private
    FDatabaseName : String;
    FDatasetName  : String;
    FRecordKey    : String;

  public
    constructor Create(const DatabaseName: String; const DatasetName: String;
                const RecordKey: String);

    property  DatabaseName: String read FDatabaseName write FDatabaseName;
    property  DatasetName: String read FDatasetName write FDatasetName;
    property  RecordKey:  String read FRecordKey write FRecordKey;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    procedure ResolveKeys(const Context: TkvScriptContext;
              var ResDatabaseName, ResDatasetName, ResRecordKey: String);
  end;

  { Field reference }

  AkvScriptFieldReference = class(AkvScriptNode)
  private
    FFieldRef : AkvScriptFieldReference;

  public
    constructor Create(const FieldRef: AkvScriptFieldReference);
    destructor Destroy; override;

    function  GetValue(const Context: TkvScriptContext; const V: AkvValue): AkvValue; virtual; abstract;
    function  Exists(const Context: TkvScriptContext; const V: AkvValue): AkvValue; virtual; abstract;
    procedure UpdateValue(const Context: TkvScriptContext; const BaseVal, Value: AkvValue); virtual; abstract;
    procedure DeleteValue(const Context: TkvScriptContext; const V: AkvValue); virtual; abstract;
    procedure InsertValue(const Context: TkvScriptContext; const BaseVal, Value: AkvValue); virtual; abstract;
    procedure AppendValue(const Context: TkvScriptContext; const BaseVal, Value: AkvValue); virtual; abstract;
  end;

  { Field name field reference }

  TkvScriptFieldNameFieldReference = class(AkvScriptFieldReference)
  private
    FFieldName : String;

  public
    constructor Create(const FieldRef: AkvScriptFieldReference; const FieldName: String);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  GetValue(const Context: TkvScriptContext; const V: AkvValue): AkvValue; override;
    function  Exists(const Context: TkvScriptContext; const V: AkvValue): AkvValue; override;
    procedure UpdateValue(const Context: TkvScriptContext; const RecVal, Value: AkvValue); override;
    procedure DeleteValue(const Context: TkvScriptContext; const V: AkvValue); override;
    procedure InsertValue(const Context: TkvScriptContext; const BaseVal, Value: AkvValue); override;
    procedure AppendValue(const Context: TkvScriptContext; const BaseVal, Value: AkvValue); override;
  end;

  { List index field reference }

  TkvScriptListIndexFieldReference = class(AkvScriptFieldReference)
  private
    FListIndex : AkvScriptExpression;

  public
    constructor Create(const FieldRef: AkvScriptFieldReference; const ListIndex: AkvScriptExpression);
    destructor Destroy; override;

    function  GetAsString: String; override;
    function  Duplicate: AkvScriptNode; override;
    function  GetValue(const Context: TkvScriptContext; const V: AkvValue): AkvValue; override;
    function  Exists(const Context: TkvScriptContext; const V: AkvValue): AkvValue; override;
    procedure UpdateValue(const Context: TkvScriptContext; const RecVal, Value: AkvValue); override;
    procedure DeleteValue(const Context: TkvScriptContext; const V: AkvValue); override;
    procedure InsertValue(const Context: TkvScriptContext; const BaseVal, Value: AkvValue); override;
    procedure AppendValue(const Context: TkvScriptContext; const BaseVal, Value: AkvValue); override;
  end;

  { Boolean operator }

  AkvScriptBinaryOperator = class(AkvScriptExpression)
  private
    FLeft  : AkvScriptExpression;
    FRight : AkvScriptExpression;

  public
    constructor Create(const Left, Right: AkvScriptExpression);
  end;

  { AND operator }

  TkvScriptANDOperator = class(AkvScriptBinaryOperator)
  public
    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { OR operator }

  TkvScriptOROperator = class(AkvScriptBinaryOperator)
  public
    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { XOR operator }

  TkvScriptXOROperator = class(AkvScriptBinaryOperator)
  public
    function  GetAsString: String; override;
    function  Duplicate: AkvScriptNode; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { NOT operator }

  TkvScriptNOTOperator = class(AkvScriptExpression)
  private
    FExpr : AkvScriptExpression;

  public
    constructor Create(const Expr: AkvScriptExpression);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Compare operator }

  TkvScriptCompareOperatorType = (
      scotEqual,
      scotNotEqual,
      scotLessThan,
      scotGreaterThan,
      scotLessOrEqualThan,
      scotGreaterOrEqualThan
    );
  TkvScriptCompareOperator = class(AkvScriptBinaryOperator)
  private
    FOp : TkvScriptCompareOperatorType;

  public
    constructor Create(const Op: TkvScriptCompareOperatorType; const Left, Right: AkvScriptExpression);
    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Plus operator }

  TkvScriptPlusOperator = class(AkvScriptBinaryOperator)
  public
    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Minus operator }

  TkvScriptMinusOperator = class(AkvScriptBinaryOperator)
  public
    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Multiply operator }

  TkvScriptMultiplyOperator = class(AkvScriptBinaryOperator)
  public
    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Divide operator }

  TkvScriptDivideOperator = class(AkvScriptBinaryOperator)
  public
    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { In operator }

  TkvScriptInOperator = class(AkvScriptBinaryOperator)
  public
    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Record and field reference }

  TkvScriptRecordAndFieldReference = class(AkvScriptNode)
  private
    FRecordRef : TkvScriptRecordReference;
    FFieldRef  : AkvScriptFieldReference;

  public
    constructor Create(const RecordRef: TkvScriptRecordReference;
                const FieldRef: AkvScriptFieldReference);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
  end;

  { Use statement }

  TkvScriptUseStatement = class(AkvScriptStatement)
  private
    FDatabaseName : String;
    FDatasetName : String;

  public
    constructor Create(const DatabaseName, DatasetName: String);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Create database statement }

  TkvScriptCreateDatabaseStatement = class(AkvScriptStatement)
  private
    FDatabaseName : String;

  public
    constructor Create(const DatabaseName: String);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Create dataset statement }

  TkvScriptCreateDatasetStatement = class(AkvScriptStatement)
  private
    FDatabaseName : String;
    FDatasetName  : String;
    FUseFolders   : Boolean;

  public
    constructor Create(const DatabaseName, DatasetName: String; const UseFolders: Boolean);
    destructor Destroy; override;
    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Drop database statement }

  TkvScriptDropDatabaseStatement = class(AkvScriptStatement)
  private
    FDatabaseName : String;

  public
    constructor Create(const DatabaseName: String);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Drop dataset statement }

  TkvScriptDropDatasetStatement = class(AkvScriptStatement)
  private
    FDatabaseName : String;
    FDatasetName : String;

  public
    constructor Create(const DatabaseName, DatasetName: String);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Drop proedure statement }

  TkvScriptDropProcedureStatement = class(AkvScriptStatement)
  private
    FDatabaseName : String;
    FProcedureName : String;

  public
    constructor Create(const DatabaseName, ProcedureName: String);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Insert statement }

  TkvScriptInsertStatement = class(AkvScriptStatement)
  private
    FRef : TkvScriptRecordAndFieldReference;
    FValue : AkvScriptExpression;

  public
    constructor Create(const Ref: TkvScriptRecordAndFieldReference;
                const Value: AkvScriptExpression);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Select expression }

  TkvScriptSelectExpression = class(AkvScriptExpression)
  private
    FRef : TkvScriptRecordAndFieldReference;

  public
    constructor Create(const Ref: TkvScriptRecordAndFieldReference);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Exists expression }

  TkvScriptExistsExpression = class(AkvScriptExpression)
  private
    FRef : TkvScriptRecordAndFieldReference;

  public
    constructor Create(const Ref: TkvScriptRecordAndFieldReference);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Delete statement }

  TkvScriptDeleteStatement = class(AkvScriptStatement)
  private
    FRef : TkvScriptRecordAndFieldReference;

  public
    constructor Create(const Ref: TkvScriptRecordAndFieldReference);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Update statement }

  TkvScriptUpdateStatement = class(AkvScriptStatement)
  private
    FRef   : TkvScriptRecordAndFieldReference;
    FValue : AkvScriptExpression;

  public
    constructor Create(const Ref: TkvScriptRecordAndFieldReference;
                const Value: AkvScriptExpression);

    function  GetAsString: String; override;
    function  Duplicate: AkvScriptNode; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Append statement }

  TkvScriptAppendStatement = class(AkvScriptStatement)
  private
    FRef   : TkvScriptRecordAndFieldReference;
    FValue : AkvScriptExpression;

  public
    constructor Create(const Ref: TkvScriptRecordAndFieldReference;
                const Value: AkvScriptExpression);

    function  GetAsString: String; override;
    function  Duplicate: AkvScriptNode; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { MkPath statement }

  TkvScriptMakePathStatement = class(AkvScriptStatement)
  private
    FRef : TkvScriptRecordReference;

  public
    constructor Create(const Ref: TkvScriptRecordReference);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Eval statement }

  TkvScriptEvalStatement = class(AkvScriptStatement)
  private
    FExpr : AkvScriptExpression;

  public
    constructor Create(const Expr: AkvScriptExpression);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { While statement }

  TkvScriptWhileStatement = class(AkvScriptStatement)
  private
    FCondition : AkvScriptExpression;
    FStatement : AkvScriptStatement;

  public
    constructor Create(const Condition: AkvScriptExpression;
                const Statement: AkvScriptStatement);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { ListOfDatabases expression }

  TkvScriptListOfDatabasesExpression = class(AkvScriptExpression)
  public
    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { ListOfDatasets expression }

  TkvScriptListOfDatasetsExpression = class(AkvScriptExpression)
  private
    FDatabaseNameExpr : AkvScriptExpression;
  public
    constructor Create(const DatabaseNameExpr: AkvScriptExpression);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Iterate statement }

  TkvScriptIterateStatement = class(AkvScriptStatement)
  private
    FDatabaseName : String;
    FDatasetName : String;
    FKeyPath : String;
    FIdentifier : String;

  public
    constructor Create(const DatabaseName, DatasetName, KeyPath, Identifier: String);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { IterateNext statement }

  TkvScriptIterateNextStatement = class(AkvScriptStatement)
  private
    FIdentifier : String;
  public
    constructor Create(const Identifier: String);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Iterator Key expression }

  TkvScriptIteratorKeyExpression = class(AkvScriptExpression)
  private
    FIdentifier : String;

  public
    constructor Create(const Identifier: String);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Iterator Value expression }

  TkvScriptIteratorValueExpression = class(AkvScriptExpression)
  private
    FIdentifier : String;

  public
    constructor Create(const Identifier: String);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Iterator Timestamp expression }

  TkvScriptIteratorTimestampExpression = class(AkvScriptExpression)
  private
    FIdentifier : String;

  public
    constructor Create(const Identifier: String);

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { List Of Keys expression }

  TkvScriptListOfKeysExpression = class(AkvScriptExpression)
  private
    FRecRef : TkvScriptRecordReference;
    FRecurse : Boolean;

  public
    constructor Create(const RecRef: TkvScriptRecordReference; const Recurse: Boolean);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Create Procedure statement }

  TkvScriptCreateProcedureParamNameArray = array of String;

  TkvScriptProcedureValue = class(AkvScriptProcedureValue)
  private
    FParamList  : TkvScriptCreateProcedureParamNameArray;
    FStatement  : AkvScriptStatement;

  public
    constructor Create(
                const ParamList: TkvScriptCreateProcedureParamNameArray;
                const Statement : AkvScriptStatement);
    destructor Destroy; override;

    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  TkvScriptProcedureScope = class(AkvScriptScope)
  private
    FParentScope : AkvScriptScope;
    FIdentifiers : TkvStringHashList;

  public
    constructor Create(const ParentScope: AkvScriptScope);
    destructor Destroy; override;

    function  GetIdentifier(const Identifier: String): TObject; override;
    procedure SetIdentifier(const Identifier: String; const Value: TObject); override;
    function  GetLocalIdentifier(const Identifier: String): TObject;
    function  ReleaseLocalIdentifier(const Identifier: String): TObject;
  end;

  TkvScriptCreateProcedureStatement = class(AkvScriptStatement)
  private
    FDatabaseName : String;
    FProcName     : String;
    FParamList    : TkvScriptCreateProcedureParamNameArray;
    FStatement    : AkvScriptStatement;

  public
    constructor Create(
                const DatabaseName: String; const ProcName: String;
                const ParamList: TkvScriptCreateProcedureParamNameArray;
                const Statement : AkvScriptStatement);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  GetScriptProcedureValue: TkvScriptProcedureValue;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Return statement }

  EkvScriptReturnSignal = class(Exception);

  TkvScriptReturnStatement = class(AkvScriptStatement)
  private
    FValueExpr : AkvScriptExpression;

  public
    constructor Create(const ValueExpr: AkvScriptExpression);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;

  { UniqueId expression }

  TkvScriptUniqueIdExpression = class(AkvScriptExpression)
  private
    FDatabaseName : String;
    FDatasetName  : String;
  public
    constructor Create(const DatabaseName, DatasetName: String);
    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Evaluate(const Context: TkvScriptContext): AkvValue; override;
  end;

  { Exec statement }

  TkvScriptExecStatement = class(AkvScriptStatement)
  private
    FScriptStrExpr : AkvScriptExpression;
  public
    constructor Create(const ScriptStrExpr: AkvScriptExpression);
    destructor Destroy; override;

    function  Duplicate: AkvScriptNode; override;
    function  GetAsString: String; override;
    function  Execute(const Context: TkvScriptContext): AkvValue; override;
  end;



implementation

uses
  Character,
  StrUtils;



{ Utility functions }

function kvScriptQuoteStr(const S: String): String;
begin
  Result := '"' + ReplaceStr(S, '"', '""') + '"';
end;

function kvScriptQuoteKey(const S: String): String;
var
  I : Integer;
  R : Boolean;
  C : Char;
begin
  R := False;
  for I := 1 to Length(S) do
    begin
      C := S[I];
      if C.IsWhiteSpace or (C = '"') then
        begin
          R := True;
          break;
        end;
    end;
  if not R then
    begin
      Result := S;
      exit;
    end;
  Result := kvScriptQuoteStr(S);
end;

function ExecScriptNode(const Context: TkvScriptContext; const Node: AkvScriptNode): AkvValue;
begin
  if Node is AkvScriptStatement then
    Result := AkvScriptStatement(Node).Execute(Context)
  else
  if Node is AkvScriptExpression then
    Result := AkvScriptExpression(Node).Evaluate(Context)
  else
    raise EkvScriptNode.Create('Not an executable node');
end;

function kvKeyIsVariable(const Key: String): Boolean;
begin
  Result := Key.StartsWith('@');
end;

function kvResolveVariableKey(const Context: TkvScriptContext; const Key: String): String;
var
  V : TObject;
begin
  if kvKeyIsVariable(Key) then
    begin
      V := Context.Scope.GetIdentifier(Key);
      if not (V is AkvValue) then
        raise EkvScriptNode.CreateFmt('Cannot resolve key: %s', [Key]);
      Result := AkvValue(V).AsString;
    end
  else
    Result := Key;
end;



{ AkvScriptStatement }

function AkvScriptStatement.DuplicateStatement: AkvScriptStatement;
begin
  Result := Duplicate as AkvScriptStatement;
end;



{ AkvScriptExpression }

function AkvScriptExpression.DuplicateExpression: AkvScriptExpression;
begin
  Result := Duplicate as AkvScriptExpression;
end;



{ AkvScriptValue }

function AkvScriptValue.Evaluate(const Context: TkvScriptContext): AkvValue;
begin
  Result := GetAsValue;
end;



{ TkvScriptBlockStatement }

constructor TkvScriptBlockStatement.Create(const List: TkvScriptNodeArray);
begin
  inherited Create;
  FList := List;
end;

function TkvScriptBlockStatement.Duplicate: AkvScriptNode;
var
  N : TkvScriptNodeArray;
  L, I : Integer;
begin
  L := Length(FList);
  SetLength(N, L);
  for I := 0 to L - 1 do
    N[I] := FList[I].Duplicate;
  Result := TkvScriptBlockStatement.Create(N);
end;

function TkvScriptBlockStatement.GetAsString: String;
var
  S : String;
  I : Integer;
begin
  S := 'BEGIN' + #13 + #10;
  for I := 0 to Length(FList) - 1 do
    S := S + FList[I].GetAsString + ';' + #13 + #10;
  S := S + 'END' + #13 + #10;
  Result := S;
end;

function TkvScriptBlockStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  I : Integer;
  V : AkvValue;
begin
  for I := 0 to Length(FList) - 1 do
    begin
      V := ExecScriptNode(Context, FList[I]);
      V.Free;
    end;
  Result := nil;
end;



{ TkvScriptIfStatement }

constructor TkvScriptIfStatement.Create(const Condition: AkvScriptExpression;
            const TrueStatement: AkvScriptNode;
            const FalseStatement: AkvScriptNode);
begin
  Assert(Assigned(Condition));
  Assert(Assigned(TrueStatement));

  inherited Create;
  FCondition := Condition;
  FTrueStatement := TrueStatement;
  FFalseStatement := FalseStatement;
end;

function TkvScriptIfStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptIfStatement.Create(
      FCondition.DuplicateExpression,
      FTrueStatement.Duplicate,
      FFalseStatement.Duplicate);
end;

function TkvScriptIfStatement.GetAsString: String;
var
  S : String;
begin
  S := 'IF ' + FCondition.GetAsString + ' THEN' + #13 + #10 +
      FTrueStatement.GetAsString + #13 + #10;
  if Assigned(FFalseStatement) then
    S := S + 'ELSE ' + FFalseStatement.GetAsString + #13 + #10;
  Result := S;
end;

function TkvScriptIfStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  V : AkvValue;
begin
  V := FCondition.Evaluate(Context);
  try
    if V.AsBoolean then
      ExecScriptNode(Context, FTrueStatement)
    else
      if Assigned(FFalseStatement) then
        ExecScriptNode(Context, FFalseStatement);
  finally
    V.Free;
  end;
  Result := nil;
end;



{ TkvScriptIfExpression }

constructor TkvScriptIfExpression.Create(const Condition: AkvScriptExpression;
            const TrueExpr: AkvScriptExpression;
            const FalseExpr: AkvScriptExpression);
begin
  Assert(Assigned(Condition));
  Assert(Assigned(TrueExpr));
  Assert(Assigned(FalseExpr));

  inherited Create;
  FCondition := Condition;
  FTrueExpr := TrueExpr;
  FFalseExpr := FalseExpr;
end;

function TkvScriptIfExpression.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptIfExpression.Create(
      FCondition.DuplicateExpression,
      FTrueExpr.DuplicateExpression,
      FFalseExpr.DuplicateExpression);
end;

function TkvScriptIfExpression.GetAsString: String;
begin
  Result :=
      'IF ' + FCondition.GetAsString +
      ' THEN ' + FTrueExpr.GetAsString +
      ' ELSE ' + FFalseExpr.GetAsString;
end;

function TkvScriptIfExpression.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  V : AkvValue;
  R : AkvValue;
begin
  V := FCondition.Evaluate(Context);
  try
    if V.AsBoolean then
      R := FTrueExpr.Evaluate(Context)
    else
      R := FFalseExpr.Evaluate(Context);
  finally
    V.Free;
  end;
  Result := R;
end;



{ AkvScriptIdentifierReference }

constructor AkvScriptIdentifierReference.Create(const IdentifierRef: AkvScriptIdentifierReference);
begin
  inherited Create;
  FIdentifierRef := IdentifierRef;
end;

destructor AkvScriptIdentifierReference.Destroy;
begin
  FreeAndNil(FIdentifierRef);
  inherited Destroy;
end;



{ TkvScriptFieldNameIdentifierReference }

constructor TkvScriptFieldNameIdentifierReference.Create(const IdentifierRef: AkvScriptIdentifierReference; const FieldName: String);
begin
  Assert(FieldName <> '');

  inherited Create(IdentifierRef);
  FFieldName := FieldName;
end;

function TkvScriptFieldNameIdentifierReference.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptFieldNameIdentifierReference.Create(
      FIdentifierRef.Duplicate as AkvScriptIdentifierReference,
      FFieldName);
end;

function TkvScriptFieldNameIdentifierReference.GetAsString: String;
var
  S : String;
begin
  if Assigned(FIdentifierRef) then
    S := FIdentifierRef.GetAsString
  else
    S := '';
  S := S + '.' + FFieldName;
  Result := S;
end;

function TkvScriptFieldNameIdentifierReference.GetValue(const Context: TkvScriptContext; const V: TObject; out Owner: Boolean): TObject;
var
  A : TObject;
  AOwn : Boolean;
begin
  if Assigned(FIdentifierRef) then
    A := FIdentifierRef.GetValue(Context, V, AOwn)
  else
    begin
      A := V;
      AOwn := False;
    end;
  try
    if not (A is TkvDictionaryValue) then
      raise EkvScriptNode.CreateFmt('Field applied to a non-dictionary: %s', [FFieldName]);
    Result := TkvDictionaryValue(A).GetValue(FFieldName);
    Owner := False;
  finally
    if AOwn then
      A.Free;
  end;
end;

procedure TkvScriptFieldNameIdentifierReference.SetValue(const Context: TkvScriptContext;
          const V: AkvValue; const Value: AkvValue);
var
  A : TObject;
  AOwn : Boolean;
  D : TkvDictionaryValue;
begin
  if Assigned(FIdentifierRef) then
    A := FIdentifierRef.GetValue(Context, V, AOwn)
  else
    begin
      A := V;
      AOwn := False;
    end;
  try
    if not (A is TkvDictionaryValue) then
      raise EkvScriptNode.CreateFmt('Field applied to a non-dictionary: %s', [FFieldName]);
    D := TkvDictionaryValue(A);
    if D.Exists(FFieldName) then
      D.SetValue(FFieldName, Value.Duplicate)
    else
      D.Add(FFieldName, Value.Duplicate)
  finally
    if AOwn then
      A.Free;
  end;
end;



{ TkvScriptListIndexIdentifierReference }

constructor TkvScriptListIndexIdentifierReference.Create(
            const IdentifierRef: AkvScriptIdentifierReference;
            const ListIndex: AkvScriptExpression);
begin
  Assert(Assigned(ListIndex));

  inherited Create(IdentifierRef);
  FListIndex := ListIndex;
end;

destructor TkvScriptListIndexIdentifierReference.Destroy;
begin
  FreeAndNil(FListIndex);
  inherited Destroy;
end;

function TkvScriptListIndexIdentifierReference.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptListIndexIdentifierReference.Create(
      FIdentifierRef.Duplicate as AkvScriptIdentifierReference,
      FListIndex.DuplicateExpression);
end;

function TkvScriptListIndexIdentifierReference.GetAsString: String;
var
  S : String;
begin
  if Assigned(FIdentifierRef) then
    S := FIdentifierRef.GetAsString
  else
    S := '';
  S := S + '[' + FListIndex.GetAsString + ']';
  Result := S;
end;

function TkvScriptListIndexIdentifierReference.GetValue(const Context: TkvScriptContext; const V: TObject; out Owner: Boolean): TObject;
var
  A : TObject;
  AOwn : Boolean;
  LIV : AkvValue;
begin
  if Assigned(FIdentifierRef) then
    A := FIdentifierRef.GetValue(Context, V, AOwn)
  else
    begin
      A := V;
      AOwn := False;
    end;
  try
    if not (A is TkvListValue) then
      raise EkvScriptNode.Create('Index applied to a non-list');
    LIV := FListIndex.Evaluate(Context);
    try
      Result := TkvListValue(A).GetValue(LIV.AsInteger);
      Owner := False;
    finally
      LIV.Free;
    end;
  finally
    if AOwn then
      A.Free;
  end;
end;

procedure TkvScriptListIndexIdentifierReference.SetValue(const Context: TkvScriptContext;
          const V: AkvValue; const Value: AkvValue);
var
  A : TObject;
  AOwn : Boolean;
  LIV : AkvValue;
  LiIdx : Integer;
  Li : TkvListValue;
begin
  if Assigned(FIdentifierRef) then
    A := FIdentifierRef.GetValue(Context, V, AOwn)
  else
    begin
      A := V;
      AOwn := False;
    end;
  try
    if not (A is TkvListValue) then
      raise EkvScriptNode.Create('Index applied to a non-list');
    Li := TkvListValue(A);
    LIV := FListIndex.Evaluate(Context);
    LiIdx := LIV.AsInteger;
    if LiIdx = -1 then
      Li.Add(Value.Duplicate)
    else
      Li.SetValue(LiIdx, Value.Duplicate);
  finally
    if AOwn then
      A.Free;
  end;
end;



{ TkvScriptFunctionCallIdentifierReference }

constructor TkvScriptFunctionCallIdentifierReference.Create(
            const IdentifierRef: AkvScriptIdentifierReference;
            const ParamExpressions: TkvScriptExpressionArray);
begin
  inherited Create(IdentifierRef);
  FParamExpressions := ParamExpressions;
end;

destructor TkvScriptFunctionCallIdentifierReference.Destroy;
var
  I : Integer;
begin
  for I := Length(FParamExpressions) - 1 downto 0 do
    FreeAndNil(FParamExpressions[I]);
  inherited Destroy;
end;

function TkvScriptFunctionCallIdentifierReference.Duplicate: AkvScriptNode;
var
  L, I : Integer;
  N : TkvScriptExpressionArray;
begin
  L := Length(FParamExpressions);
  SetLength(N, L);
  for I := 0 to L - 1 do
    N[I] := FParamExpressions[I].DuplicateExpression;
  Result := TkvScriptFunctionCallIdentifierReference.Create(
      FIdentifierRef.Duplicate as AkvScriptIdentifierReference, N);
end;

function TkvScriptFunctionCallIdentifierReference.GetAsString: String;
var
  S : String;
  I : Integer;
begin
  if Assigned(FIdentifierRef) then
    S := FIdentifierRef.GetAsString
  else
    S := '';
  S := S + '(';
  for I := 0 to Length(FParamExpressions) - 1 do
    begin
      if I > 0 then
        S := S + ', ';
      S := S + FParamExpressions[I].GetAsString;
    end;
  S := S + ')';
  Result := S;
end;

function TkvScriptFunctionCallIdentifierReference.GetValue(const Context: TkvScriptContext;
         const V: TObject; out Owner: Boolean): TObject;
var
  A : TObject;
  AOwn : Boolean;
  L, I : Integer;
  PR : AkvScriptProcedureValue;
  PV : TkvValueArray;
  Res : AkvValue;
begin
  if Assigned(FIdentifierRef) then
    A := FIdentifierRef.GetValue(Context, V, AOwn)
  else
    begin
      A := V;
      AOwn := False;
    end;
  try
    if not (A is AkvScriptProcedureValue) then
      raise EkvScriptNode.Create('Function call to a non-procedure');
    PR := AkvScriptProcedureValue(A);
    L := Length(FParamExpressions);
    SetLength(PV, L);
    for I := 0 to L - 1 do
      PV[I] := nil;
    try
      for I := 0 to L - 1 do
        PV[I] := FParamExpressions[I].Evaluate(Context);
      Res := PR.Call(Context, PV);
    finally
      for I := L - 1 downto 0 do
        PV[I].Free;
    end;
    Result := Res;
    Owner := True;
  finally
    if AOwn then
      A.Free;
  end;
end;

procedure TkvScriptFunctionCallIdentifierReference.SetValue(const Context: TkvScriptContext;
          const V, Value: AkvValue);
begin
  raise EkvScriptNode.Create('Cannot set value on identifier call');
end;



{ TkvIdentifierExpression }

constructor TkvScriptIdentifierExpression.Create(const Identifier: String;
            const IdentiferRef: AkvScriptIdentifierReference);
begin
  inherited Create;
  FIdentifier := Identifier;
  FIdentifierRef := IdentiferRef;
end;

destructor TkvScriptIdentifierExpression.Destroy;
begin
  FreeAndNil(FIdentifierRef);
  inherited Destroy;
end;

function TkvScriptIdentifierExpression.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptIdentifierExpression.Create(
      FIdentifier,
      FIdentifierRef as AkvScriptIdentifierReference);
end;

function TkvScriptIdentifierExpression.GetAsString: String;
begin
  Result := FIdentifier;
end;

function TkvScriptIdentifierExpression.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  V : TObject;
  R : TObject;
  ROwn : Boolean;
begin
  V := Context.Scope.GetIdentifier(FIdentifier);
  if not Assigned(FIdentifierRef) then
    begin
      if V is AkvScriptProcedureValue then
        Result := AkvScriptProcedureValue(V).Call(Context, nil)
      else
        begin
          if not (V is AkvValue) then
            raise EkvScriptNode.Create('Identifier value type not recognised');
          Result := AkvValue(V).Duplicate;
        end
    end
  else
    begin
      R := FIdentifierRef.GetValue(Context, V, ROwn);
      try
        if not (R is AkvValue) then
          raise EkvScriptNode.Create('Identifier reference not a value');
        if ROwn then
          begin
            Result := AkvValue(R);
            ROwn := False;
          end
        else
          Result := AkvValue(R).Duplicate;
      finally
        if ROwn then
          R.Free;
      end;
    end;
end;



{ TkvScriptSetStatement }

constructor TkvScriptSetStatement.Create(const Identifier: String;
            const IdentifierRef: AkvScriptIdentifierReference;
            const Value: AkvScriptExpression);
begin
  Assert(Assigned(Value));

  inherited Create;
  FIdentifier := Identifier;
  FIdentifierRef := IdentifierRef;
  FValue := Value;
end;

destructor TkvScriptSetStatement.Destroy;
begin
  FreeAndNil(FValue);
  FreeAndNil(FIdentifierRef);
  inherited Destroy;
end;

function TkvScriptSetStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptSetStatement.Create(
      FIdentifier,
      FIdentifierRef.Duplicate as AkvScriptIdentifierReference,
      FValue.DuplicateExpression);
end;

function TkvScriptSetStatement.GetAsString: String;
begin
  Result := 'SET ' + FIdentifier + ' = ' + FValue.GetAsString;
end;

function TkvScriptSetStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  Val : AkvValue;
  V : TObject;
begin
  Val := FValue.Evaluate(Context);
  if not Assigned(FIdentifierRef) then
    Context.Scope.SetIdentifier(FIdentifier, Val)
  else
    begin
      V := Context.Scope.GetIdentifier(FIdentifier);
      if not (V is AkvValue) then
        raise EkvScriptNode.Create('Identifier value type not recognised');
      FIdentifierRef.SetValue(Context, AkvValue(V), Val);
    end;
  Result := nil;
end;



{ TkvScriptNegateValue }

constructor TkvScriptNegateValue.Create(const Value: AkvScriptValue);
begin
  Assert(Assigned(Value));

  inherited Create;
  FValue := Value;
end;

function TkvScriptNegateValue.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptNegateValue.Create(
      FValue.Duplicate as AkvScriptValue);
end;

function TkvScriptNegateValue.GetAsString: String;
begin
  Result := '-' + FValue.GetAsString;
end;

function TkvScriptNegateValue.GetAsValue: AkvValue;
begin
  Result := FValue.GetAsValue;
  Result.Negate;
end;

function TkvScriptNegateValue.Evaluate(const Context: TkvScriptContext): AkvValue;
begin
  Result := FValue.Evaluate(Context);
  Result.Negate;
end;



{ TkvScriptStringValue }

constructor TkvScriptStringValue.Create(const Value: String);
begin
  inherited Create;
  FValue := Value;
end;

function TkvScriptStringValue.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptStringValue.Create(FValue);
end;

function TkvScriptStringValue.GetAsString: String;
begin
  Result := kvScriptQuoteStr(FValue);
end;

function TkvScriptStringValue.GetAsValue: AkvValue;
begin
  Result := TkvStringValue.Create(FValue);
end;



{ TkvScriptIntegerValue }

constructor TkvScriptIntegerValue.Create(const Value: Int64);
begin
  inherited Create;
  FValue := Value;
end;

function TkvScriptIntegerValue.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptIntegerValue.Create(FValue);
end;

function TkvScriptIntegerValue.GetAsString: String;
begin
  Result := IntToStr(FValue);
end;

function TkvScriptIntegerValue.GetAsValue: AkvValue;
begin
  Result := TkvIntegerValue.Create(FValue);
end;



{ TkvScriptFloatValue }

constructor TkvScriptFloatValue.Create(const Value: Double);
begin
  inherited Create;
  FValue := Value;
end;

function TkvScriptFloatValue.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptFloatValue.Create(FValue);
end;

function TkvScriptFloatValue.GetAsString: String;
begin
  Result := FloatToStr(FValue);
end;

function TkvScriptFloatValue.GetAsValue: AkvValue;
begin
  Result := TkvFloatValue.Create(FValue);
end;



{ TkvScriptBooleanValue }

constructor TkvScriptBooleanValue.Create(const Value: Boolean);
begin
  inherited Create;
  FValue := Value;
end;

function TkvScriptBooleanValue.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptBooleanValue.Create(FValue);
end;

function TkvScriptBooleanValue.GetAsString: String;
begin
  if FValue then
    Result := 'true'
  else
    Result := 'false';
end;

function TkvScriptBooleanValue.GetAsValue: AkvValue;
begin
  Result := TkvBooleanValue.Create(FValue);
end;



{ TkvScriptDateTimeValue }

constructor TkvScriptDateTimeValue.Create(const Value: TDateTime);
begin
  inherited Create;
  FValue := Value;
end;

function TkvScriptDateTimeValue.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptDateTimeValue.Create(FValue);
end;

function TkvScriptDateTimeValue.GetAsString: String;
begin
  Result := 'DATETIME("' + DateTimeToStr(FValue) + '")';
end;

function TkvScriptDateTimeValue.GetAsValue: AkvValue;
begin
  Result := TkvDateTimeValue.Create(FValue);
end;



{ TkvScriptNullValue }

function TkvScriptNullValue.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptNullValue.Create;
end;

function TkvScriptNullValue.GetAsString: String;
begin
  Result := 'null';
end;

function TkvScriptNullValue.GetAsValue: AkvValue;
begin
  Result := TkvNullValue.Create;
end;



{ TkvScriptDictionaryValue }

constructor TkvScriptDictionaryValue.Create(const Value: TkvScriptDictionaryKeyValueArray);
begin
  inherited Create;
  FValue := Value;
end;

destructor TkvScriptDictionaryValue.Destroy;
var
  I : Integer;
begin
  for I := Length(FValue) - 1 downto 0 do
    FreeAndNil(FValue[I].Value);
  inherited Destroy;
end;

function TkvScriptDictionaryValue.Duplicate: AkvScriptNode;
var
  N : TkvScriptDictionaryKeyValueArray;
  L, I : Integer;
begin
  L := Length(FValue);
  SetLength(N, L);
  for I := 0 to L - 1 do
    N[I] := FValue[I];
  Result := TkvScriptDictionaryValue.Create(N);
end;

function TkvScriptDictionaryValue.GetAsString: String;
var
  I : Integer;
  S : String;
begin
  S := '{';
  for I := 0 to Length(FValue) - 1 do
    begin
      if I > 0 then
        S := S + ',';
      S := S + kvScriptQuoteKey(FValue[I].Key) + ':' +
          FValue[I].Value.GetAsString;
    end;
  S := S + '}';
  Result := S;
end;

function TkvScriptDictionaryValue.GetAsValue: AkvValue;
var
  D : TkvDictionaryValue;
  I : Integer;
begin
  D := TkvDictionaryValue.Create;
  for I := 0 to Length(FValue) - 1 do
    D.Add(FValue[I].Key, FValue[I].Value.Evaluate(nil));
  Result := D;
end;



{ TkvScriptListValue }

constructor TkvScriptListValue.Create(const Value: TkvScriptListValueArray);
begin
  inherited Create;
  FValue := Value;
end;

destructor TkvScriptListValue.Destroy;
var
  I : Integer;
begin
  for I := Length(FValue) - 1 downto 0 do
    FreeAndNil(FValue[I]);
  inherited Destroy;
end;

function TkvScriptListValue.Duplicate: AkvScriptNode;
var
  L, I : Integer;
  N : TkvScriptListValueArray;
begin
  L := Length(FValue);
  SetLength(N, L);
  for I := 0 to L - 1 do
    N[I] := FValue[I].DuplicateExpression;
  Result := TkvScriptListValue.Create(N);
end;

function TkvScriptListValue.GetAsString: String;
var
  I : Integer;
  S : String;
begin
  S := '[';
  for I := 0 to Length(FValue) - 1 do
    begin
      if I > 0 then
        S := S + ',';
      S := S + FValue[I].GetAsString;
    end;
  S := S + ']';
  Result := S;
end;

function TkvScriptListValue.GetAsValue: AkvValue;
var
  I : Integer;
  L : TkvListValue;
begin
  L := TkvListValue.Create;
  for I := 0 to Length(FValue) - 1 do
    L.Add(FValue[I].Evaluate(nil));
  Result := L;
end;



{ TkvScriptRecordReference }

constructor TkvScriptRecordReference.Create(
            const DatabaseName, DatasetName, RecordKey: String);
begin
  inherited Create;
  FDatabaseName := DatabaseName;
  FDatasetName := DatasetName;
  FRecordKey := RecordKey;
end;

function TkvScriptRecordReference.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptRecordReference.Create(FDatabaseName,
      FDatasetName, FRecordKey);
end;

function TkvScriptRecordReference.GetAsString: String;
var
  S : String;
begin
  if FDatabaseName <> '' then
    S := kvScriptQuoteKey(FDatabaseName) + ':'
  else
    S := '';
  if FDatasetName <> '' then
    S := S + kvScriptQuoteKey(FDatasetName) + '\';
  if FRecordKey <> '' then
    S := S + kvScriptQuoteKey(FRecordKey);
  Result := S;
end;

procedure TkvScriptRecordReference.ResolveKeys(const Context: TkvScriptContext;
          var ResDatabaseName, ResDatasetName, ResRecordKey: String);
begin
  ResDatabaseName := kvResolveVariableKey(Context, FDatabaseName);
  ResDatasetName := kvResolveVariableKey(Context, FDatasetName);
  ResRecordKey := kvResolveVariableKey(Context, FRecordKey);
end;



{ AkvScriptFieldReference }

constructor AkvScriptFieldReference.Create(const FieldRef: AkvScriptFieldReference);
begin
  inherited Create;
  FFieldRef := FieldRef;
end;

destructor AkvScriptFieldReference.Destroy;
begin
  FreeAndNil(FFieldRef);
  inherited Destroy;
end;



{ TkvScriptFieldNameFieldReference }

constructor TkvScriptFieldNameFieldReference.Create(
            const FieldRef: AkvScriptFieldReference;
            const FieldName: String);
begin
  Assert(FieldName <> '');

  inherited Create(FieldRef);
  FFieldName := FieldName;
end;

function TkvScriptFieldNameFieldReference.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptFieldNameFieldReference.Create(
      FFieldRef.Duplicate as AkvScriptFieldReference, FFieldName);
end;

function TkvScriptFieldNameFieldReference.GetAsString: String;
var
  S : String;
begin
  if Assigned(FFieldRef) then
    S := FFieldRef.GetAsString;
  S := S + '.' + kvScriptQuoteKey(FFieldName);
  Result := S;
end;

function TkvScriptFieldNameFieldReference.GetValue(const Context: TkvScriptContext; const V: AkvValue): AkvValue;
var
  A : AkvValue;
begin
  if Assigned(FFieldRef) then
    A := FFieldRef.GetValue(Context, V)
  else
    A := V;
  if not (A is TkvDictionaryValue) then
    raise EkvScriptNode.CreateFmt('Field applied to a non-dictionary: %s', [FFieldName]);
  Result := TkvDictionaryValue(A).GetValue(FFieldName);
end;

function TkvScriptFieldNameFieldReference.Exists(const Context: TkvScriptContext; const V: AkvValue): AkvValue;
var
  A : AkvValue;
begin
  if Assigned(FFieldRef) then
    A := FFieldRef.GetValue(Context, V)
  else
    A := V;
  if not (A is TkvDictionaryValue) then
    raise EkvScriptNode.CreateFmt('Field applied to a non-dictionary: %s', [FFieldName]);
  Result := TkvBooleanValue.Create(TkvDictionaryValue(A).Exists(FFieldName));
end;

procedure TkvScriptFieldNameFieldReference.UpdateValue(const Context: TkvScriptContext; const RecVal, Value: AkvValue);
var
  A : AkvValue;
begin
  if Assigned(FFieldRef) then
    A := FFieldRef.GetValue(Context, RecVal)
  else
    A := RecVal;
  if not (A is TkvDictionaryValue) then
    raise EkvScriptNode.CreateFmt('Field applied to a non-dictionary: %s', [FFieldName]);
  TkvDictionaryValue(A).SetValue(FFieldName, Value.Duplicate);
end;

procedure TkvScriptFieldNameFieldReference.DeleteValue(const Context: TkvScriptContext; const V: AkvValue);
var
  A : AkvValue;
begin
  if Assigned(FFieldRef) then
    A := FFieldRef.GetValue(Context, V)
  else
    A := V;
  if not (A is TkvDictionaryValue) then
    raise EkvScriptNode.CreateFmt('Field applied to a non-dictionary: %s', [FFieldName]);
  TkvDictionaryValue(A).DeleteKey(FFieldName);
end;

procedure TkvScriptFieldNameFieldReference.InsertValue(const Context: TkvScriptContext; const BaseVal, Value: AkvValue);
var
  A : AkvValue;
  D, E : TkvDictionaryValue;
  Itr : TkvDictionaryValueIterator;
  ItrKey : String;
  ItrVal : AkvValue;
begin
  if Assigned(FFieldRef) then
    A := FFieldRef.GetValue(Context, BaseVal)
  else
    A := BaseVal;
  if not (A is TkvDictionaryValue) then
    raise EkvScriptNode.CreateFmt('Field applied to a non-dictionary: %s', [FFieldName]);

  if FFieldName <> '@' then
    begin
      A := TkvDictionaryValue(A).GetValue(FFieldName);
      if not (A is TkvDictionaryValue) then
        raise EkvScriptNode.CreateFmt('Field applied to a non-dictionary: %s', [FFieldName]);
    end;
  E := TkvDictionaryValue(A);

  if not (Value is TkvDictionaryValue) then
    raise EkvScriptNode.Create('Value must be a dictionary');
  D := TkvDictionaryValue(Value);

  if D.IterateFirst(Itr) then
    repeat
      D.IteratorGetKeyValue(Itr, ItrKey, ItrVal);
      E.Add(ItrKey, ItrVal.Duplicate);
    until not D.IterateNext(Itr);
end;

procedure TkvScriptFieldNameFieldReference.AppendValue(
          const Context: TkvScriptContext; const BaseVal, Value: AkvValue);
var
  A, B : AkvValue;
begin
  if Assigned(FFieldRef) then
    A := FFieldRef.GetValue(Context, BaseVal)
  else
    A := BaseVal;
  if not (A is TkvDictionaryValue) then
    raise EkvScriptNode.CreateFmt('Field applied to a non-dictionary: %s', [FFieldName]);

  B := TkvDictionaryValue(A).GetValue(FFieldName);

  TkvDictionaryValue(A).SetValue(FFieldName, ValueOpAppend(B, Value));
end;



{ TkvScriptListIndexFieldReference }

constructor TkvScriptListIndexFieldReference.Create(
            const FieldRef: AkvScriptFieldReference;
            const ListIndex: AkvScriptExpression);
begin
  Assert(Assigned(ListIndex));

  inherited Create(FieldRef);
  FListIndex := ListIndex;
end;

destructor TkvScriptListIndexFieldReference.Destroy;
begin
  FreeAndNil(FListIndex);
  inherited Destroy;
end;

function TkvScriptListIndexFieldReference.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptListIndexFieldReference.Create(
      FFieldRef.Duplicate as AkvScriptFieldReference,
      FListIndex.DuplicateExpression);
end;

function TkvScriptListIndexFieldReference.GetAsString: String;
var
  S : String;
begin
  if Assigned(FFieldRef) then
    S := FFieldRef.GetAsString;
  S := S + '[' + FListIndex.GetAsString + ']';
  Result := S;
end;

function TkvScriptListIndexFieldReference.GetValue(const Context: TkvScriptContext; const V: AkvValue): AkvValue;
var
  A : AkvValue;
  LIV : AkvValue;
begin
  if Assigned(FFieldRef) then
    A := FFieldRef.GetValue(Context, V)
  else
    A := V;
  if not (A is TkvListValue) then
    raise EkvScriptNode.Create('Index applied to a non-list');
  LIV := FListIndex.Evaluate(Context);
  try
    Result := TkvListValue(A).GetValue(LIV.AsInteger);
  finally
    LIV.Free;
  end;
end;

function TkvScriptListIndexFieldReference.Exists(const Context: TkvScriptContext; const V: AkvValue): AkvValue;
var
  A : AkvValue;
  LIV : AkvValue;
  LI : Integer;
  R : Boolean;
begin
  if Assigned(FFieldRef) then
    A := FFieldRef.GetValue(Context, V)
  else
    A := V;
  if not (A is TkvListValue) then
    raise EkvScriptNode.Create('Index applied to a non-list');
  LIV := FListIndex.Evaluate(Context);
  try
    LI := LIV.AsInteger;
    R := (LI >= 0) and (LI < TkvListValue(A).GetCount);
    Result := TkvBooleanValue.Create(R);
  finally
    LIV.Free;
  end;
end;

procedure TkvScriptListIndexFieldReference.UpdateValue(const Context: TkvScriptContext; const RecVal, Value: AkvValue);
var
  A : AkvValue;
  LIV : AkvValue;
begin
  if Assigned(FFieldRef) then
    A := FFieldRef.GetValue(Context, RecVal)
  else
    A := RecVal;
  if not (A is TkvListValue) then
    raise EkvScriptNode.Create('Index applied to a non-list');
  LIV := FListIndex.Evaluate(Context);
  TkvListValue(A).SetValue(LIV.AsInteger, Value.Duplicate);
end;

procedure TkvScriptListIndexFieldReference.DeleteValue(const Context: TkvScriptContext; const V: AkvValue);
var
  A : AkvValue;
  LIV : AkvValue;
begin
  if Assigned(FFieldRef) then
    A := FFieldRef.GetValue(Context, V)
  else
    A := V;
  if not (A is TkvListValue) then
    raise EkvScriptNode.Create('Index applied to a non-list');
  LIV := FListIndex.Evaluate(Context);
  TkvListValue(A).DeleteValue(LIV.AsInteger);
end;

procedure TkvScriptListIndexFieldReference.InsertValue(const Context: TkvScriptContext; const BaseVal, Value: AkvValue);
var
  A : AkvValue;
  LIV : AkvValue;
  LI : Integer;
begin
  if Assigned(FFieldRef) then
    A := FFieldRef.GetValue(Context, BaseVal)
  else
    A := BaseVal;
  if not (A is TkvListValue) then
    raise EkvScriptNode.Create('Index applied to a non-list');
  LIV := FListIndex.Evaluate(Context);
  LI := LIV.AsInteger;
  if LI = -1 then
    TkvListValue(A).AppendValue(Value.Duplicate)
  else
    TkvListValue(A).InsertValue(LI, Value.Duplicate);
end;

procedure TkvScriptListIndexFieldReference.AppendValue(const Context: TkvScriptContext; const BaseVal, Value: AkvValue);
var
  A : AkvValue;
  LIV : AkvValue;
  LI : Integer;
  V, N : AkvValue;
begin
  if Assigned(FFieldRef) then
    A := FFieldRef.GetValue(Context, BaseVal)
  else
    A := BaseVal;
  if not (A is TkvListValue) then
    raise EkvScriptNode.Create('Index applied to a non-list');
  LIV := FListIndex.Evaluate(Context);
  LI := LIV.AsInteger;
  V := TkvListValue(A).GetValue(LI);
  N := ValueOpAppend(V, Value);
  TkvListValue(A).SetValue(LI, N);
  N.Free;
end;



{ AkvScriptBinaryOperator }

constructor AkvScriptBinaryOperator.Create(const Left, Right: AkvScriptExpression);
begin
  Assert(Assigned(Left));
  Assert(Assigned(Right));

  inherited Create;
  FLeft := Left;
  FRight := Right;
end;


{ TkvScriptANDOperator }

function TkvScriptANDOperator.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptANDOperator.Create(
      FLeft.DuplicateExpression,
      FRight.DuplicateExpression);
end;

function TkvScriptANDOperator.GetAsString: String;
begin
  Result := '(' + FLeft.GetAsString + ' AND ' + FRight.GetAsString + ')';
end;

function TkvScriptANDOperator.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  L, R : AkvValue;
begin
  L := FLeft.Evaluate(Context);
  try
    if L is TkvBooleanValue then
      if not L.AsBoolean then
        begin
          Result := TkvBooleanValue.Create(False);
          exit;
        end;
    R := FRight.Evaluate(Context);
    try
      Result := ValueOpAND(L, R);
    finally
      R.Free;
    end;
  finally
    L.Free;
  end;
end;



{ TkvScriptOROperator }

function TkvScriptOROperator.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptOROperator.Create(
      FLeft.DuplicateExpression,
      FRight.DuplicateExpression);
end;

function TkvScriptOROperator.GetAsString: String;
begin
  Result := '(' + FLeft.GetAsString + ' OR ' + FRight.GetAsString + ')';
end;

function TkvScriptOROperator.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  L, R : AkvValue;
begin
  L := FLeft.Evaluate(Context);
  try
    if L is TkvBooleanValue then
      if L.AsBoolean then
        begin
          Result := TkvBooleanValue.Create(True);
          exit;
        end;
    R := FRight.Evaluate(Context);
    try
      Result := ValueOpOR(L, R);
    finally
      R.Free;
    end;
  finally
    L.Free;
  end;
end;



{ TkvScriptXOROperator }

function TkvScriptXOROperator.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptXOROperator.Create(
      FLeft.DuplicateExpression,
      FRight.DuplicateExpression);
end;

function TkvScriptXOROperator.GetAsString: String;
begin
  Result := '(' + FLeft.GetAsString + ' XOR ' + FRight.GetAsString + ')';
end;

function TkvScriptXOROperator.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  L, R : AkvValue;
begin
  L := FLeft.Evaluate(Context);
  try
    R := FRight.Evaluate(Context);
    try
      Result := ValueOpXOR(L, R);
    finally
      R.Free;
    end;
  finally
    L.Free;
  end;
end;



{ TkvScriptNOTOperator }

constructor TkvScriptNOTOperator.Create(const Expr: AkvScriptExpression);
begin
  Assert(Assigned(Expr));

  inherited Create;
  FExpr := Expr;
end;

destructor TkvScriptNOTOperator.Destroy;
begin
  FreeAndNil(FExpr);
  inherited Destroy;
end;

function TkvScriptNOTOperator.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptNOTOperator.Create(FExpr.DuplicateExpression);
end;

function TkvScriptNOTOperator.GetAsString: String;
begin
  Result := 'NOT (' + FExpr.GetAsString + ')';
end;

function TkvScriptNOTOperator.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  V : AkvValue;
begin
  V := FExpr.Evaluate(Context);
  try
    Result := ValueOpNOT(V);
  finally
    V.Free;
  end;
end;



{ TkvScriptCompareOperator }

const
  ScriptCompareOpStr: array[TkvScriptCompareOperatorType] of String = (
      '=', '<>', '<', '>', '<=', '>=');

constructor TkvScriptCompareOperator.Create(const Op: TkvScriptCompareOperatorType;
  const Left, Right: AkvScriptExpression);
begin
  inherited Create(Left, Right);
  FOp := Op;
end;

function TkvScriptCompareOperator.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptCompareOperator.Create(
      FOp,
      FLeft.DuplicateExpression,
      FRight.DuplicateExpression);
end;

function TkvScriptCompareOperator.GetAsString: String;
begin
  Result := '(' + FLeft.GetAsString + ' ' + ScriptCompareOpStr[FOp] + ' ' + FRight.GetAsString + ')';
end;

function TkvScriptCompareOperator.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  L, R : AkvValue;
  Cmp : Integer;
  Res : Boolean;
begin
  L := FLeft.Evaluate(Context);
  try
    R := FRight.Evaluate(Context);
    try
      Cmp := ValueOpCompare(L, R);
      case FOp of
        scotEqual              : Res := Cmp = 0;
        scotNotEqual           : Res := Cmp <> 0;
        scotLessThan           : Res := Cmp < 0;
        scotGreaterThan        : Res := Cmp > 0;
        scotLessOrEqualThan    : Res := Cmp <= 0;
        scotGreaterOrEqualThan : Res := Cmp >= 0;
      else
        Res := False;
      end;
      Result := TkvBooleanValue.Create(Res);
    finally
      R.Free;
    end;
  finally
    L.Free;
  end;
end;



{ TkvScriptPlusOperator }

function TkvScriptPlusOperator.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptPlusOperator.Create(
      FLeft.DuplicateExpression,
      FRight.DuplicateExpression);
end;

function TkvScriptPlusOperator.GetAsString: String;
begin
  Result := '(' + FLeft.GetAsString + ' + ' + FRight.GetAsString + ')';
end;

function TkvScriptPlusOperator.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  L, R : AkvValue;
begin
  L := FLeft.Evaluate(Context);
  try
    R := FRight.Evaluate(Context);
    try
      Result := ValueOpPlus(L, R);
    finally
      R.Free;
    end;
  finally
    L.Free;
  end;
end;



{ TkvScriptMinusOperator }

function TkvScriptMinusOperator.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptMinusOperator.Create(
      FLeft.DuplicateExpression,
      FRight.DuplicateExpression);
end;

function TkvScriptMinusOperator.GetAsString: String;
begin
  Result := '(' + FLeft.GetAsString + ' + ' + FRight.GetAsString + ')';
end;

function TkvScriptMinusOperator.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  L, R : AkvValue;
begin
  L := FLeft.Evaluate(Context);
  try
    R := FRight.Evaluate(Context);
    try
      Result := ValueOpMinus(L, R);
    finally
      R.Free;
    end;
  finally
    L.Free;
  end;
end;



{ TkvScriptMultiplyOperator }

function TkvScriptMultiplyOperator.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptMultiplyOperator.Create(
      FLeft.DuplicateExpression,
      FRight.DuplicateExpression);
end;

function TkvScriptMultiplyOperator.GetAsString: String;
begin
  Result := '(' + FLeft.GetAsString + ' * ' + FRight.GetAsString + ')';
end;

function TkvScriptMultiplyOperator.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  L, R : AkvValue;
begin
  L := FLeft.Evaluate(Context);
  try
    R := FRight.Evaluate(Context);
    try
      Result := ValueOpMultiply(L, R);
    finally
      R.Free;
    end;
  finally
    L.Free;
  end;
end;



{ TkvScriptDivideOperator }

function TkvScriptDivideOperator.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptDivideOperator.Create(
      FLeft.DuplicateExpression,
      FRight.DuplicateExpression);
end;

function TkvScriptDivideOperator.GetAsString: String;
begin
  Result := '(' + FLeft.GetAsString + ' / ' + FRight.GetAsString + ')';
end;

function TkvScriptDivideOperator.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  L, R : AkvValue;
begin
  L := FLeft.Evaluate(Context);
  try
    R := FRight.Evaluate(Context);
    try
      Result := ValueOpDivide(L, R);
    finally
      R.Free;
    end;
  finally
    L.Free;
  end;
end;



{ TkvScriptInOperator }

function TkvScriptInOperator.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptInOperator.Create(
      FLeft.DuplicateExpression,
      FRight.DuplicateExpression);
end;

function TkvScriptInOperator.GetAsString: String;
begin
  Result := '(' + FLeft.GetAsString + ' IN ' + FRight.GetAsString + ')';
end;

function TkvScriptInOperator.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  L, R : AkvValue;
begin
  L := FLeft.Evaluate(Context);
  try
    R := FRight.Evaluate(Context);
    try
      Result := TkvBooleanValue.Create(ValueOpIn(L, R));
    finally
      R.Free;
    end;
  finally
    L.Free;
  end;
end;



{ TkvScriptRecordAndFieldReference }

constructor TkvScriptRecordAndFieldReference.Create(const RecordRef: TkvScriptRecordReference;
            const FieldRef: AkvScriptFieldReference);
begin
  inherited Create;
  FRecordRef := RecordRef;
  FFieldRef := FieldRef;
end;

destructor TkvScriptRecordAndFieldReference.Destroy;
begin
  FreeAndNil(FFieldRef);
  FreeAndNil(FRecordRef);
  inherited Destroy;
end;

function TkvScriptRecordAndFieldReference.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptRecordAndFieldReference.Create(
     FRecordRef.Duplicate as TkvScriptRecordReference,
     FFieldRef.Duplicate as AkvScriptFieldReference);
end;

function TkvScriptRecordAndFieldReference.GetAsString: String;
var
  S : String;
begin
  S := FRecordRef.GetAsString;
  if Assigned(FFieldRef) then
    S := S + FFieldRef.GetAsString;
  Result := S;
end;



{ TkvScriptUseStatement }

constructor TkvScriptUseStatement.Create(const DatabaseName, DatasetName: String);
begin
  inherited Create;
  FDatabaseName := DatabaseName;
  FDatasetName := DatasetName;
end;

function TkvScriptUseStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptUseStatement.Create(
      FDatabaseName,
      FDatasetName);
end;

function TkvScriptUseStatement.GetAsString: String;
var
  S : String;
begin
  S := 'USE ';
  if (FDatabaseName = '') and (FDatasetName = '') then
    S := S + '*'
  else
    begin
      S := S + kvScriptQuoteKey(FDatabaseName);
      if FDatasetName <> '' then
        S := S + ':' + kvScriptQuoteKey(FDatasetName);
    end;
  Result := S;
end;

function TkvScriptUseStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  DsN : String;
  DbN : String;
begin
  DbN := kvResolveVariableKey(Context, FDatabaseName);
  DsN := kvResolveVariableKey(Context, FDatasetName);
  if DsN = '' then
    if DbN = '' then
      Context.Session.UseNone
    else
      Context.Session.UseDatabase(DbN)
  else
    Context.Session.UseDataset(DbN, DsN);
  Result := nil;
end;



{ TkvScriptCreateDatabaseStatement }

constructor TkvScriptCreateDatabaseStatement.Create(const DatabaseName: String);
begin
  Assert(DatabaseName <> '');

  inherited Create;
  FDatabaseName := DatabaseName;
end;

function TkvScriptCreateDatabaseStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptCreateDatabaseStatement.Create(FDatabaseName);
end;

function TkvScriptCreateDatabaseStatement.GetAsString: String;
begin
  Result := 'CREATE DATABASE ' + kvScriptQuoteKey(FDatabaseName);
end;

function TkvScriptCreateDatabaseStatement.Execute(const Context: TkvScriptContext): AkvValue;
begin
  if FDatabaseName = '' then
    raise EkvScriptNode.Create('Database not specified');
  Context.Session.CreateDatabase(FDatabaseName);
  Result := nil;
end;



{ TkvScriptCreateDatasetStatement }

constructor TkvScriptCreateDatasetStatement.Create(const DatabaseName, DatasetName: String;
            const UseFolders: Boolean);
begin
  Assert(DatasetName <> '');

  inherited Create;
  FDatabaseName := DatabaseName;
  FDatasetName := DatasetName;
  FUseFolders := UseFolders;
end;

destructor TkvScriptCreateDatasetStatement.Destroy;
begin
  inherited Destroy;
end;

function TkvScriptCreateDatasetStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptCreateDatasetStatement.Create(
      FDatabaseName, FDatasetName, FUseFolders);
end;

function TkvScriptCreateDatasetStatement.GetAsString: String;
var
  S : String;
begin
  S := 'CREATE DATASET ';
  if FDatabaseName <> '' then
    S := S + kvScriptQuoteKey(FDatabaseName) + ':';
  S := S + kvScriptQuoteKey(FDatasetName);
  if FUseFolders then
    S := S + ' WITH_FOLDERS'
  else
    S := S + ' WITHOUT_FOLDERS';
  Result := S;
end;

function TkvScriptCreateDatasetStatement.Execute(const Context: TkvScriptContext): AkvValue;
begin
  if FDatasetName = '' then
    raise EkvScriptNode.Create('Dataset not specified');
  Context.Session.CreateDataset(FDatabaseName, FDatasetName, FUseFolders);
  Result := nil;
end;



{ TkvScriptDropDatabaseStatement }

constructor TkvScriptDropDatabaseStatement.Create(const DatabaseName: String);
begin
  inherited Create;
  FDatabaseName := DatabaseName;
end;

function TkvScriptDropDatabaseStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptDropDatabaseStatement.Create(FDatabaseName);
end;

function TkvScriptDropDatabaseStatement.GetAsString: String;
begin
  Result := 'DROP DATABASE ' + kvScriptQuoteKey(FDatabaseName);
end;

function TkvScriptDropDatabaseStatement.Execute(const Context: TkvScriptContext): AkvValue;
begin
  if FDatabaseName = '' then
    raise EkvScriptNode.Create('Database not specified');
  Context.Session.DropDatabase(FDatabaseName);
  Result := nil;
end;



{ TkvScriptDropDatasetStatement }

constructor TkvScriptDropDatasetStatement.Create(const DatabaseName, DatasetName: String);
begin
  inherited Create;
  FDatabaseName := DatabaseName;
  FDatasetName := DatasetName;
end;

destructor TkvScriptDropDatasetStatement.Destroy;
begin
  inherited Destroy;
end;

function TkvScriptDropDatasetStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptDropDatasetStatement.Create(FDatabaseName, FDatasetName);
end;

function TkvScriptDropDatasetStatement.GetAsString: String;
var
  S : String;
begin
  S := 'DROP DATASET ';
  if FDatabaseName <> '' then
    S := S + kvScriptQuoteKey(FDatabaseName) + ':';
  S := S + kvScriptQuoteKey(FDatasetName);
  Result := S;
end;

function TkvScriptDropDatasetStatement.Execute(const Context: TkvScriptContext): AkvValue;
begin
  if FDatasetName = '' then
    raise EkvScriptNode.Create('Dataset not specified');
  Context.Session.DropDataset(FDatabaseName, FDatasetName);
  Result := nil;
end;



{ TkvScriptDropProcedureStatement }

constructor TkvScriptDropProcedureStatement.Create(const DatabaseName, ProcedureName: String);
begin
  Assert(DatabaseName <> '');
  Assert(ProcedureName <> '');
  inherited Create;
  FDatabaseName := DatabaseName;
  FProcedureName := ProcedureName;
end;

destructor TkvScriptDropProcedureStatement.Destroy;
begin
  inherited Destroy;
end;

function TkvScriptDropProcedureStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptDropProcedureStatement.Create(FDatabaseName, FProcedureName);
end;

function TkvScriptDropProcedureStatement.GetAsString: String;
var
  S : String;
begin
  S := 'DROP PROCEDURE ';
  S := S + kvScriptQuoteKey(FDatabaseName) + ':';
  S := S + kvScriptQuoteKey(FProcedureName);
  Result := S;
end;

function TkvScriptDropProcedureStatement.Execute(const Context: TkvScriptContext): AkvValue;
begin
  Context.Session.DropStoredProcedure(FDatabaseName, FProcedureName);
  Result := nil;
end;



{ TkvScriptInsertStatement }

constructor TkvScriptInsertStatement.Create(const Ref: TkvScriptRecordAndFieldReference;
  const Value: AkvScriptExpression);
begin
  Assert(Assigned(Value));

  inherited Create;
  FRef := Ref;
  FValue := Value;
end;

destructor TkvScriptInsertStatement.Destroy;
begin
  FreeAndNil(FValue);
  FreeAndNil(FRef);
  inherited Destroy;
end;

function TkvScriptInsertStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptInsertStatement.Create(
      FRef.Duplicate as TkvScriptRecordAndFieldReference,
      FValue.DuplicateExpression);
end;

function TkvScriptInsertStatement.GetAsString: String;
begin
  Result := 'INSERT ' + FRef.GetAsString + ' ' + FValue.GetAsString;
end;

function TkvScriptInsertStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  RecRef : TkvScriptRecordReference;
  FieldRef : AkvScriptFieldReference;
  V, A : AkvValue;
  ResDb, ResDs, ResRec : String;
begin
  RecRef := FRef.FRecordRef;
  FieldRef := FRef.FFieldRef;
  V := FValue.Evaluate(Context);
  try
    RecRef.ResolveKeys(Context, ResDb, ResDs, ResRec);
    if not Assigned(FieldRef) then
      begin
        Context.Session.AddRecord(ResDb, ResDs, ResRec, V);
        Result := nil;
        exit;
      end;

    Context.Session.ExecLock;
    try
      A := Context.Session.GetRecord(ResDb, ResDs, ResRec);
      try
        FieldRef.InsertValue(Context, A, V);
        Context.Session.SetRecord(ResDb, ResDs, ResRec, A);
      finally
        A.Free;
      end;
    finally
      Context.Session.ExecUnlock;
    end;

  finally
    V.Free;
  end;
  Result := nil;
end;



{ TkvScriptSelectExpression }

constructor TkvScriptSelectExpression.Create(const Ref: TkvScriptRecordAndFieldReference);
begin
  inherited Create;
  FRef := Ref;
end;

function TkvScriptSelectExpression.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptSelectExpression.Create(
      FRef.Duplicate as TkvScriptRecordAndFieldReference);
end;

function TkvScriptSelectExpression.GetAsString: String;
begin
  Result := 'SELECT ' + FRef.GetAsString;
end;

function TkvScriptSelectExpression.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  V, R : AkvValue;
  RecRef : TkvScriptRecordReference;
  FieldRef : AkvScriptFieldReference;
  ResDb, ResDs, ResRec : String;
begin
  RecRef := FRef.FRecordRef;
  FieldRef := FRef.FFieldRef;

  RecRef.ResolveKeys(Context, ResDb, ResDs, ResRec);
  V := Context.Session.GetRecord(ResDb, ResDs, ResRec);
  if not Assigned(FieldRef) then
    begin
      Result := V;
      exit;
    end;

  try
    R := FieldRef.GetValue(Context, V).Duplicate;
  finally
    V.Free;
  end;

  Result := R;
end;



{ TkvScriptExistsExpression }

constructor TkvScriptExistsExpression.Create(const Ref: TkvScriptRecordAndFieldReference);
begin
  inherited Create;
  FRef := Ref;
end;

function TkvScriptExistsExpression.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptExistsExpression.Create(
      FRef.Duplicate as TkvScriptRecordAndFieldReference);
end;

function TkvScriptExistsExpression.GetAsString: String;
begin
  Result := 'EXISTS ' + FRef.GetAsString;
end;

function TkvScriptExistsExpression.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  V, R : AkvValue;
  RecRef : TkvScriptRecordReference;
  FieldRef : AkvScriptFieldReference;
  ResDb, ResDs, ResRec : String;
begin
  RecRef := FRef.FRecordRef;
  FieldRef := FRef.FFieldRef;

  RecRef.ResolveKeys(Context, ResDb, ResDs, ResRec);
  if not Assigned(FieldRef) then
    begin
      Result := TkvBooleanValue.Create(
          Context.Session.RecordExists(ResDb, ResDs, ResRec));
      exit;
    end;

  V := Context.Session.GetRecord(ResDb, ResDs, ResRec);
  try
    R := FieldRef.Exists(Context, V);
  finally
    V.Free;
  end;

  Result := R;
end;



{ TkvScriptDeleteStatement }

constructor TkvScriptDeleteStatement.Create(const Ref: TkvScriptRecordAndFieldReference);
begin
  inherited Create;
  FRef := Ref;
end;

function TkvScriptDeleteStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptDeleteStatement.Create(
      FRef.Duplicate as TkvScriptRecordAndFieldReference);
end;

destructor TkvScriptDeleteStatement.Destroy;
begin
  FreeAndNil(FRef);
  inherited Destroy;
end;

function TkvScriptDeleteStatement.GetAsString: String;
begin
  Result := 'DELETE ' + FRef.GetAsString;
end;

function TkvScriptDeleteStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  RecRef : TkvScriptRecordReference;
  FieldRef : AkvScriptFieldReference;
  A : AkvValue;
  ResDb, ResDs, ResRec : String;
begin
  RecRef := FRef.FRecordRef;
  FieldRef := FRef.FFieldRef;

  RecRef.ResolveKeys(Context, ResDb, ResDs, ResRec);
  if not Assigned(FieldRef) then
    begin
      Context.Session.DeleteRecord(ResDb, ResDs, ResRec);
      Result := nil;
      exit;
    end;

  Context.Session.ExecLock;
  try
    A := Context.Session.GetRecord(ResDb, ResDs, ResRec);
    try
      FieldRef.DeleteValue(Context, A);
      Context.Session.SetRecord(ResDb, ResDs, ResRec, A);
    finally
      A.Free;
    end;
  finally
    Context.Session.ExecUnlock;
  end;
  Result := nil;
end;



{ TkvScriptUpdateStatement }

constructor TkvScriptUpdateStatement.Create(const Ref: TkvScriptRecordAndFieldReference;
  const Value: AkvScriptExpression);
begin
  Assert(Assigned(Value));

  inherited Create;
  FRef := Ref;
  FValue := Value;
end;

function TkvScriptUpdateStatement.GetAsString: String;
begin
  Result := 'UPDATE ' + FRef.GetAsString + ' ' + FValue.GetAsString;
end;

function TkvScriptUpdateStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptUpdateStatement.Create(
      FRef.Duplicate as TkvScriptRecordAndFieldReference,
      FValue.DuplicateExpression);
end;

function TkvScriptUpdateStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  V, A : AkvValue;
  RecRef : TkvScriptRecordReference;
  FieldRef : AkvScriptFieldReference;
  ResDb, ResDs, ResRec : String;
begin
  RecRef := FRef.FRecordRef;
  FieldRef := FRef.FFieldRef;

  V := FValue.Evaluate(Context);
  try
    RecRef.ResolveKeys(Context, ResDb, ResDs, ResRec);

    if not Assigned(FieldRef) then
      begin
        Context.Session.SetRecord(ResDb, ResDs, ResRec, V);
        Result := nil;
        exit;
      end;

    Context.Session.ExecLock;
    try
      A := Context.Session.GetRecord(ResDb, ResDs, ResRec);
      try
        FieldRef.UpdateValue(Context, A, V);
        Context.Session.SetRecord(ResDb, ResDs, ResRec, A);
      finally
        A.Free;
      end;
    finally
      Context.Session.ExecUnlock;
    end;

  finally
    V.Free;
  end;
  Result := nil;
end;



{ TkvScriptAppendStatement }

constructor TkvScriptAppendStatement.Create(const Ref: TkvScriptRecordAndFieldReference;
  const Value: AkvScriptExpression);
begin
  Assert(Assigned(Value));

  inherited Create;
  FRef := Ref;
  FValue := Value;
end;

function TkvScriptAppendStatement.GetAsString: String;
begin
  Result := 'APPEND ' + FRef.GetAsString + ' ' + FValue.GetAsString;
end;

function TkvScriptAppendStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptAppendStatement.Create(
      FRef.Duplicate as TkvScriptRecordAndFieldReference,
      FValue.DuplicateExpression);
end;

function TkvScriptAppendStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  V, A : AkvValue;
  RecRef : TkvScriptRecordReference;
  FieldRef : AkvScriptFieldReference;
  ResDb, ResDs, ResRec : String;
begin
  RecRef := FRef.FRecordRef;
  FieldRef := FRef.FFieldRef;
  V := FValue.Evaluate(Context);
  try
    RecRef.ResolveKeys(Context, ResDb, ResDs, ResRec);

    if not Assigned(FieldRef) then
      begin
        Context.Session.AppendRecord(ResDb, ResDs, ResRec, V);
        Result := nil;
        exit;
      end;

    Context.Session.ExecLock;
    try
      A := Context.Session.GetRecord(ResDb, ResDs, ResRec);
      try
        FieldRef.AppendValue(Context, A, V);
        Context.Session.SetRecord(ResDb, ResDs, ResRec, A);
      finally
        A.Free;
      end;
    finally
      Context.Session.ExecUnlock;
    end;

  finally
    V.Free;
  end;
  Result := nil;
end;



{ TkvScriptMkPathStatement }

constructor TkvScriptMakePathStatement.Create(const Ref: TkvScriptRecordReference);
begin
  inherited Create;
  FRef := Ref;
end;

function TkvScriptMakePathStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptMakePathStatement.Create(
      FRef.Duplicate as TkvScriptRecordReference);
end;

destructor TkvScriptMakePathStatement.Destroy;
begin
  FreeAndNil(FRef);
  inherited Destroy;
end;

function TkvScriptMakePathStatement.GetAsString: String;
begin
  Result := 'MKPATH ' + FRef.GetAsString;
end;

function TkvScriptMakePathStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  ResDb, ResDs, ResRec : String;
begin
  FRef.ResolveKeys(Context, ResDb, ResDs, ResRec);
  Context.Session.MakePath(ResDb, ResDs, ResRec);
  Result := nil;
end;



{ TkvScriptEvalStatement }

constructor TkvScriptEvalStatement.Create(const Expr: AkvScriptExpression);
begin
  Assert(Assigned(Expr));

  inherited Create;
  FExpr := Expr;
end;

destructor TkvScriptEvalStatement.Destroy;
begin
  FreeAndNil(FExpr);
  inherited Destroy;
end;

function TkvScriptEvalStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptEvalStatement.Create(
      FExpr.DuplicateExpression);
end;

function TkvScriptEvalStatement.GetAsString: String;
begin
  Result := 'EVAL ' + FExpr.GetAsString;
end;

function TkvScriptEvalStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  V : AkvValue;
begin
  V := FExpr.Evaluate(Context);
  Result := V;
end;



{ TkvScriptWhileStatement }

constructor TkvScriptWhileStatement.Create(const Condition: AkvScriptExpression;
                const Statement: AkvScriptStatement);
begin
  Assert(Assigned(Condition));
  Assert(Assigned(Statement));

  inherited Create;
  FCondition := Condition;
  FStatement := Statement;
end;

destructor TkvScriptWhileStatement.Destroy;
begin
  FreeAndNil(FStatement);
  FreeAndNil(FCondition);
end;

function TkvScriptWhileStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptWhileStatement.Create(
      FCondition.DuplicateExpression,
      FStatement.DuplicateStatement);
end;

function TkvScriptWhileStatement.GetAsString: String;
begin
  Result := 'WHILE ' + FCondition.GetAsString + #13 + #10 +
      FStatement.GetAsString;
end;

function TkvScriptWhileStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  CondV : AkvValue;
  CondB : Boolean;
  V : AkvValue;
begin
  repeat
    CondV := FCondition.Evaluate(Context);
    try
      CondB := CondV.AsBoolean;
    finally
      CondV.Free;
    end;
    if CondB then
      begin
        V := FStatement.Execute(Context);
        V.Free;
      end;
  until not CondB;
  Result := nil;
end;


{ TkvListOfDatabasesExpression }

function TkvScriptListOfDatabasesExpression.GetAsString: String;
begin
  Result := 'LISTOF DATABASES';
end;

function TkvScriptListOfDatabasesExpression.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptListOfDatabasesExpression.Create;
end;

function TkvScriptListOfDatabasesExpression.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  A : TkvKeyNameArray;
  R : TkvListValue;
  I : Integer;
begin
  A := Context.Session.ListOfDatabases;
  R := TkvListValue.Create;
  for I := 0 to Length(A) - 1 do
    R.Add(TkvStringValue.Create(A[I]));
  Result := R;
end;



{ TkvListOfDatasetsExpression }

constructor TkvScriptListOfDatasetsExpression.Create(const DatabaseNameExpr: AkvScriptExpression);
begin
  inherited Create;
  FDatabaseNameExpr := DatabaseNameExpr;
end;

destructor TkvScriptListOfDatasetsExpression.Destroy;
begin
  FreeAndNil(FDatabaseNameExpr);
  inherited Destroy;
end;

function TkvScriptListOfDatasetsExpression.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptListOfDatasetsExpression.Create(
      FDatabaseNameExpr.DuplicateExpression);
end;

function TkvScriptListOfDatasetsExpression.GetAsString: String;
begin
  Result := 'LISTOF DATASETS(' + FDatabaseNameExpr.GetAsString + ')';
end;

function TkvScriptListOfDatasetsExpression.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  N : AkvValue;
  DbN : String;
  A : TkvKeyNameArray;
  R : TkvListValue;
  I : Integer;
begin
  N := FDatabaseNameExpr.Evaluate(Context);
  try
    DbN := N.AsString;
  finally
    N.Free;
  end;
  A := Context.Session.ListOfDatasets(DbN);
  R := TkvListValue.Create;
  for I := 0 to Length(A) - 1 do
    R.Add(TkvStringValue.Create(A[I]));
  Result := R;
end;



{ TkvDatasetIteratorValue }

type
  TkvDatasetIteratorValue = class(TkvBooleanValue)
  private
    FIterator : TkvDatasetIterator;
  protected
    function  GetAsBoolean: Boolean; override;
  public
    constructor Create(const Iterator: TkvDatasetIterator);
    destructor Destroy; override;
    function  Duplicate: AkvValue; override;
  end;

constructor TkvDatasetIteratorValue.Create(const Iterator: TkvDatasetIterator);
begin
  inherited Create;
  FIterator := Iterator;
end;

destructor TkvDatasetIteratorValue.Destroy;
begin
  inherited Destroy;
end;

function TkvDatasetIteratorValue.GetAsBoolean: Boolean;
begin
  Result := FIterator.Dataset.IteratorHasRecord(FIterator);
end;

function TkvDatasetIteratorValue.Duplicate: AkvValue;
begin
  Result := TkvDatasetIteratorValue.Create(FIterator);
end;



{ TkvScriptIterateStatement }

constructor TkvScriptIterateStatement.Create(
            const DatabaseName, DatasetName, KeyPath, Identifier: String);
begin
  Assert(Identifier <> '');

  inherited Create;
  FDatabaseName := DatabaseName;
  FDatasetName := DatasetName;
  FKeyPath := KeyPath;
  FIdentifier := Identifier;
end;

function TkvScriptIterateStatement.GetAsString: String;
var
  S : String;
begin
  S := 'ITERATE ';
  if FDatabaseName <> '' then
    S := S + FDatabaseName + ':';
  S := S + FDatasetName + ' ' + FIdentifier;
  Result := S;
end;

function TkvScriptIterateStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptIterateStatement.Create(
      FDatabaseName, FDatasetName, FKeyPath, FIdentifier);
end;

function TkvScriptIterateStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  DbN : String;
  DsN : String;
  Iterator : TkvDatasetIterator;
  ItVal : TkvDatasetIteratorValue;
begin
  DbN := kvResolveVariableKey(Context, FDatabaseName);
  DsN := kvResolveVariableKey(Context, FDatasetName);
  Context.Session.IterateRecords(DbN, DsN, FKeyPath, Iterator);
  ItVal := TkvDatasetIteratorValue.Create(Iterator);
  Context.Scope.SetIdentifier(FIdentifier, ItVal);
  Result := nil;
end;



{ TkvScriptIterateNextStatement }

constructor TkvScriptIterateNextStatement.Create(const Identifier: String);
begin
  Assert(Identifier <> '');

  inherited Create;
  FIdentifier := Identifier;
end;

function TkvScriptIterateNextStatement.GetAsString: String;
begin
  Result := 'ITERATE_NEXT ' + FIdentifier;
end;

function TkvScriptIterateNextStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptIterateNextStatement.Create(FIdentifier);
end;

function TkvScriptIterateNextStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  It : TObject;
  ItV : TkvDatasetIteratorValue;
begin
  It := Context.Scope.GetIdentifier(FIdentifier);
  if not (It is TkvDatasetIteratorValue) then
    raise EkvScriptNode.Create('Not an iterator');
  ItV := TkvDatasetIteratorValue(It);
  Context.Session.IterateNextRecord(ItV.FIterator);
  Result := nil;
end;



{ TkvScriptIteratorKeyExpression }

constructor TkvScriptIteratorKeyExpression.Create(const Identifier: String);
begin
  Assert(Identifier <> '');

  inherited Create;
  FIdentifier := Identifier;
end;

function TkvScriptIteratorKeyExpression.GetAsString: String;
begin
  Result := 'ITERATOR_KEY ' + FIdentifier;
end;

function TkvScriptIteratorKeyExpression.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptIteratorKeyExpression.Create(FIdentifier);
end;

function TkvScriptIteratorKeyExpression.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  It : TObject;
  ItV : TkvDatasetIteratorValue;
  KeyS : String;
begin
  It := Context.Scope.GetIdentifier(FIdentifier);
  if not (It is TkvDatasetIteratorValue) then
    raise EkvScriptNode.Create('Not an iterator');
  ItV := TkvDatasetIteratorValue(It);
  KeyS := Context.Session.IteratorGetKey(ItV.FIterator);
  Result := TkvStringValue.Create(KeyS);
end;



{ TkvScriptIteratorValueExpression }

constructor TkvScriptIteratorValueExpression.Create(const Identifier: String);
begin
  Assert(Identifier <> '');

  inherited Create;
  FIdentifier := Identifier;
end;

function TkvScriptIteratorValueExpression.GetAsString: String;
begin
  Result := 'ITERATOR_VALUE ' + FIdentifier;
end;

function TkvScriptIteratorValueExpression.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptIteratorValueExpression.Create(FIdentifier);
end;

function TkvScriptIteratorValueExpression.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  It : TObject;
  ItV : TkvDatasetIteratorValue;
  Val : AkvValue;
begin
  It := Context.Scope.GetIdentifier(FIdentifier);
  if not (It is TkvDatasetIteratorValue) then
    raise EkvScriptNode.Create('Not an iterator');
  ItV := TkvDatasetIteratorValue(It);
  Val := Context.Session.IteratorGetValue(ItV.FIterator);
  Result := Val;
end;



{ TkvScriptIteratorTimestampExpression }

constructor TkvScriptIteratorTimestampExpression.Create(const Identifier: String);
begin
  Assert(Identifier <> '');

  inherited Create;
  FIdentifier := Identifier;
end;

function TkvScriptIteratorTimestampExpression.GetAsString: String;
begin
  Result := 'ITERATOR_TIMESTAMP ' + FIdentifier;
end;

function TkvScriptIteratorTimestampExpression.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptIteratorTimestampExpression.Create(FIdentifier);
end;

function TkvScriptIteratorTimestampExpression.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  It : TObject;
  ItV : TkvDatasetIteratorValue;
  Val : Int64;
begin
  It := Context.Scope.GetIdentifier(FIdentifier);
  if not (It is TkvDatasetIteratorValue) then
    raise EkvScriptNode.Create('Not an iterator');
  ItV := TkvDatasetIteratorValue(It);
  Val := Context.Session.IteratorGetTimestamp(ItV.FIterator);
  Result := TkvIntegerValue.Create(Val);
end;



{ TkvScriptListOfKeysExpression }

constructor TkvScriptListOfKeysExpression.Create(const RecRef: TkvScriptRecordReference;
            const Recurse: Boolean);
begin
  inherited Create;
  FRecRef := RecRef;
  FRecurse := Recurse;
end;

destructor TkvScriptListOfKeysExpression.Destroy;
begin
  FreeAndNil(FRecRef);
  inherited Create;
end;

function TkvScriptListOfKeysExpression.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptListOfKeysExpression.Create(
      FRecRef.Duplicate as TkvScriptRecordReference,
      FRecurse);
end;

function TkvScriptListOfKeysExpression.GetAsString: String;
var
  S : String;
begin
  S := 'LIST_OF_KEYS ' + FRecRef.GetAsString;
  if FRecurse then
    S := S + ' RECURSE';
  Result := S;
end;

function TkvScriptListOfKeysExpression.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  ResDb, ResDs, ResRec : String;
begin
  FRecRef.ResolveKeys(Context, ResDb, ResDs, ResRec);
  Result := Context.Session.ListOfKeys(ResDb, ResDs, ResRec, FRecurse);
end;



{ TkvScriptProcedureScope }

constructor TkvScriptProcedureScope.Create(const ParentScope: AkvScriptScope);
begin
  inherited Create;
  FParentScope := ParentScope;
  FIdentifiers := TkvStringHashList.Create(False, False, True);
end;

destructor TkvScriptProcedureScope.Destroy;
begin
  FreeAndNil(FIdentifiers);
  inherited Destroy;
end;

function TkvScriptProcedureScope.GetIdentifier(const Identifier: String): TObject;
var
  F : Boolean;
  R : TObject;
begin
  F := FIdentifiers.GetValue(Identifier, R);
  if not F then
    if Assigned(FParentScope) then
      begin
        Result := FParentScope.GetIdentifier(Identifier);
        exit;
      end
    else
      raise EkvScriptScope.CreateFmt('Identifier not defined: %s', [Identifier]);
  Result := R;
end;

procedure TkvScriptProcedureScope.SetIdentifier(const Identifier: String;
          const Value: TObject);
begin
  if FIdentifiers.KeyExists(Identifier) then
    FIdentifiers.SetValue(Identifier, Value)
  else
    FIdentifiers.Add(Identifier, Value)
end;

function TkvScriptProcedureScope.GetLocalIdentifier(const Identifier: String): TObject;
var
  R : TObject;
begin
  if not FIdentifiers.GetValue(Identifier, R) then
    Result := nil
  else
    Result := R;
end;

function TkvScriptProcedureScope.ReleaseLocalIdentifier(const Identifier: String): TObject;
var
  R : TObject;
begin
  if not FIdentifiers.RemoveKey(Identifier, R) then
    Result := nil
  else
    Result := R;
end;



{ TkvProcedureValue }

constructor TkvScriptProcedureValue.Create(
            const ParamList: TkvScriptCreateProcedureParamNameArray;
            const Statement: AkvScriptStatement);
begin
  Assert(Assigned(Statement));

  inherited Create;
  FParamList := ParamList;
  FStatement := Statement;
end;

destructor TkvScriptProcedureValue.Destroy;
begin
  FreeAndNil(FStatement);
  inherited Destroy;
end;

function TkvScriptProcedureValue.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  L, I : Integer;
  C : TkvScriptContext;
  S : TkvScriptProcedureScope;
  Res : TObject;
  ResV : AkvValue;
  DbN : String;
  DsN : String;
begin
  L := Length(ParamValues);
  if L <> Length(FParamList) then
    raise EkvScriptNode.Create('Parameter count mismatch');
  DbN := Context.Session.GetSelectedDatabaseName;
  DsN := Context.Session.GetSelectedDatasetName;
  S := TkvScriptProcedureScope.Create(Context.Scope);
  try
    for I := 0 to L - 1 do
      S.SetIdentifier(FParamList[I], ParamValues[I].Duplicate);
    C := TkvScriptContext.Create(S, sstStoredProcedure, Context.Session);
    try
      try
        FStatement.Execute(C);
      except
        on E : EkvScriptReturnSignal do ;
      else
        raise;
      end;

      Res := S.ReleaseLocalIdentifier('__RESULT__');
      if Assigned(Res) then
        begin
          if not (Res is AkvValue) then
            raise EkvScriptNode.Create('Invalid procedure result type');
          ResV := AkvValue(Res);
        end
      else
        ResV := nil;

      if (DbN <> Context.Session.GetSelectedDatabaseName) or
         (DsN <> Context.Session.GetSelectedDatasetName) then
        if DbN <> '' then
          if DsN <> '' then
            Context.Session.UseDataset(DbN, DsN)
          else
            Context.Session.UseDatabase(DbN)
        else
          Context.Session.UseNone;
    finally
      C.Free;
    end;
  finally
    S.Free;
  end;
  Result := ResV;
end;



{ TkvScriptProcedureDefinitionStatement }

constructor TkvScriptCreateProcedureStatement.Create(
            const DatabaseName: String;
            const ProcName: String;
            const ParamList: TkvScriptCreateProcedureParamNameArray;
            const Statement: AkvScriptStatement);
begin
  Assert(ProcName <> '');
  Assert(Assigned(Statement));

  inherited Create;
  FDatabaseName := DatabaseName;
  FProcName := ProcName;
  FParamList := ParamList;
  FStatement := Statement;
end;

destructor TkvScriptCreateProcedureStatement.Destroy;
begin
  FreeAndNil(FStatement);
  inherited Destroy;
end;

function TkvScriptCreateProcedureStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptCreateProcedureStatement.Create(
      FDatabaseName,
      FProcName,
      Copy(FParamList),
      FStatement.DuplicateStatement);
end;

function TkvScriptCreateProcedureStatement.GetAsString: String;
var
  S : String;
  L, I : Integer;
begin
  S := 'CREATE PROCEDURE ';
  if FDatabaseName <> '' then
    S := S + FDatabaseName + ':';
  S := S + FProcName;
  L := Length(FParamList);
  if L > 0 then
    begin
      S := S + '(';
      for I := 0 to L - 1 do
        begin
          if I > 0 then
            S := S + ', ';
          S := S + FParamList[I];
        end;
      S := S + ')';
    end;
  S := S + #13#10;
  S := S + FStatement.GetAsString;
  Result := S;
end;

function TkvScriptCreateProcedureStatement.GetScriptProcedureValue: TkvScriptProcedureValue;
begin
  Result := TkvScriptProcedureValue.Create(Copy(FParamList), FStatement.DuplicateStatement);
end;

function TkvScriptCreateProcedureStatement.Execute(const Context: TkvScriptContext): AkvValue;
begin
  if FDatabaseName <> '' then
    Context.Session.CreateStoredProcedure(FDatabaseName, FProcName, GetAsString)
  else
    Context.Scope.SetIdentifier(FProcName, GetScriptProcedureValue);
  Result := nil;
end;



{ TkvScriptReturnStatement }

constructor TkvScriptReturnStatement.Create(const ValueExpr: AkvScriptExpression);
begin
  inherited Create;
  FValueExpr := ValueExpr;
end;

destructor TkvScriptReturnStatement.Destroy;
begin
  FreeAndNil(FValueExpr);
  inherited Destroy;
end;

function TkvScriptReturnStatement.Duplicate: AkvScriptNode;
begin
  if Assigned(FValueExpr) then
    Result := TkvScriptReturnStatement.Create(FValueExpr.DuplicateExpression)
  else
    Result := TkvScriptReturnStatement.Create(nil);
end;

function TkvScriptReturnStatement.GetAsString: String;
var
  S : String;
begin
  S := 'RETURN';
  if Assigned(FValueExpr) then
    S := S + ' ' + FValueExpr.GetAsString;
  Result := S;
end;

function TkvScriptReturnStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  RetV : AkvValue;
begin
  if Context.ScopeType <> sstStoredProcedure then
    raise EkvScriptNode.Create('RETURN only allowed in stored procedure');
  RetV := FValueExpr.Evaluate(Context);
  Context.Scope.SetIdentifier('__RESULT__', RetV);
  raise EkvScriptReturnSignal.Create('');
end;



{ TkvScriptUniqueIdExpression }

constructor TkvScriptUniqueIdExpression.Create(const DatabaseName, DatasetName: String);
begin
  inherited Create;
  FDatabaseName := DatabaseName;
  FDatasetName := DatasetName;
end;

function TkvScriptUniqueIdExpression.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptUniqueIdExpression.Create(FDatabaseName, FDatasetName);
end;

function TkvScriptUniqueIdExpression.GetAsString: String;
var
  S : String;
begin
  S := 'UNIQUE_ID';
  if FDatabaseName <> '' then
    begin
      S := S + ' ' + FDatabaseName;
      if FDatasetName <> '' then
        S := S + ':' + FDatasetName;
    end;
  Result := S;
end;

function TkvScriptUniqueIdExpression.Evaluate(const Context: TkvScriptContext): AkvValue;
var
  DsN : String;
  DbN : String;
  U : UInt64;
begin
  DbN := kvResolveVariableKey(Context, FDatabaseName);
  DsN := kvResolveVariableKey(Context, FDatasetName);
  if DbN <> '' then
    if DsN <> '' then
      U := Context.Session.AllocateDatasetUniqueId(DbN, DsN)
    else
      U := Context.Session.AllocateDatabaseUniqueId(DbN)
  else
    U := Context.Session.AllocateSystemUniqueId;
  Result := TkvIntegerValue.Create(U);
end;



{ TkvScriptExecStatement }

constructor TkvScriptExecStatement.Create(const ScriptStrExpr: AkvScriptExpression);
begin
  inherited Create;
  FScriptStrExpr := ScriptStrExpr;
end;

destructor TkvScriptExecStatement.Destroy;
begin
  FreeAndNil(FScriptStrExpr);
  inherited Destroy;
end;

function TkvScriptExecStatement.Duplicate: AkvScriptNode;
begin
  Result := TkvScriptExecStatement.Create(FScriptStrExpr.DuplicateExpression);
end;

function TkvScriptExecStatement.GetAsString: String;
begin
  Result := 'EXEC ' + FScriptStrExpr.GetAsString;
end;

function TkvScriptExecStatement.Execute(const Context: TkvScriptContext): AkvValue;
var
  ScriptV : AkvValue;
  ScriptS : String;
begin
  ScriptV := FScriptStrExpr.Evaluate(Context);
  try
    ScriptS := ScriptV.AsString;
    Result := Context.Session.ExecScript(ScriptS);
  finally
    ScriptV.Free;
  end;
end;



end.

