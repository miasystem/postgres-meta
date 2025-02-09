import type {
  PostgresColumn,
  PostgresMaterializedView,
  PostgresSchema,
  PostgresTable,
  PostgresType,
  PostgresView,
} from '../../lib/index.js'
import type { GeneratorMetadata } from '../../lib/generators.js'
import { Console } from 'console'
const console = new Console(process.stderr)

type Operation = 'Select' | 'Insert' | 'Update'

function formatForDartClassName(name: string): string {
  return name
    .split(/[^a-zA-Z0-9]/)
    .map((word) => `${word[0].toUpperCase()}${word.slice(1)}`)
    .join('')
}

function formatForDartPropertyName(name: string): string {
  const className = formatForDartClassName(name)
  return className[0].toLowerCase() + className.slice(1)
}

interface Typeable {
  generateType(): string
}

interface Declarable {
  generateDeclaration(): string
}

interface JsonEncodable {
  generateJsonEncoding(): string
}

interface JsonDecodable {
  generateJsonDecoding(inputParameter: string): string
}

type DartType = Typeable & JsonEncodable & JsonDecodable

type BuiltinDartTypeKeyword = 'int' | 'double' | 'bool' | 'String' | 'dynamic'

class BuiltinDartType implements DartType {
  keyword: BuiltinDartTypeKeyword

  constructor(keyword: BuiltinDartTypeKeyword) {
    this.keyword = keyword
  }

  generateType(): string {
    return this.keyword
  }

  generateJsonEncoding(): string {
    return ''
  }

  generateJsonDecoding(inputParameter: string): string {
    return `${inputParameter} as ${this.keyword}`
  }
}

class DatetimeDartType implements DartType {
  generateType(): string {
    return 'DateTime'
  }

  generateJsonEncoding(): string {
    return '.toIso8601String()'
  }

  generateJsonDecoding(inputParameter: string): string {
    return `DateTime.parse(${inputParameter})`
  }
}

class DurationDartType implements DartType {
  generateType(): string {
    return 'Duration'
  }

  generateJsonEncoding(): string {
    return '.inSeconds'
  }

  generateJsonDecoding(inputParameter: string): string {
    return `parsePostgresInterval(${inputParameter})`
  }
}

class ListDartType implements DartType {
  containedType: DartType

  constructor(containedType: DartType) {
    this.containedType = containedType
  }

  generateType(): string {
    return `List<${this.containedType.generateType()}>`
  }

  generateJsonEncoding(): string {
    return `.map((v) => v${this.containedType.generateJsonEncoding()}).toList()`
  }

  generateJsonDecoding(inputParameter: string): string {
    return `(${inputParameter} as List<dynamic>).map((v) => ${this.containedType.generateJsonDecoding('v')}).toList()`
  }
}

class NullDartType implements DartType {
  containedType: DartType

  constructor(containedType: DartType) {
    if (containedType instanceof NullDartType) {
      this.containedType = containedType.containedType
    } else {
      this.containedType = containedType
    }
  }
  generateType(): string {
    return `${this.containedType.generateType()}?`
  }

  generateJsonEncoding(): string {
    return `${this.containedType.generateJsonEncoding()}`
  }

  generateJsonDecoding(inputParameter: string): string {
    return `${inputParameter} == null ? null : ${this.containedType.generateJsonDecoding(inputParameter)}`
  }
}

class MapDartType implements DartType {
  keyType: DartType
  valueType: DartType

  constructor(keyType: DartType, valueType: DartType) {
    this.keyType = keyType
    this.valueType = valueType
  }

  generateType(): string {
    return `Map<${this.keyType.generateType()}, ${this.valueType.generateType()}>`
  }

  generateJsonEncoding(): string {
    return ''
  }

  generateJsonDecoding(inputParameter: string): string {
    return `${inputParameter} as Map<String, dynamic>`
  }
}

class EnumDartConstruct implements DartType, Declarable {
  originalName: string
  values: string[]

  constructor(name: string, values: string[], comment: string | null) {
    this.originalName = name
    this.values = values
  }

  generateType(): string {
    return formatForDartClassName(this.originalName)
  }

  generateJsonEncoding(): string {
    return '.toJson()'
  }

  generateJsonDecoding(inputParameter: string): string {
    return `${formatForDartClassName(this.originalName)}.fromJson(${inputParameter})`
  }

  generateDeclaration(): string {
    return `enum ${formatForDartClassName(this.originalName)} {
${this.values.map((v) => `  ${formatForDartPropertyName(v)}`).join(',\n')};

  String toJson() {
    switch(this) {
${this.values
  .map(
    (v) =>
      `     case ${formatForDartClassName(this.originalName)}.${formatForDartPropertyName(v)}:
        return '${v}';`
  )
  .join('\n')}
    }
  }

  factory ${formatForDartClassName(this.originalName)}.fromJson(String name) {
    switch(name) {
${this.values
  .map(
    (v) =>
      `     case '${v}':
        return ${formatForDartClassName(this.originalName)}.${formatForDartPropertyName(v)};`
  )
  .join('\n')}
    }
    throw ArgumentError.value(name, "name", "No enum value with that name");
  }
}
`
  }
}

class ClassDartConstructForCompositeType implements DartType, Declarable {
  postgresType: PostgresType
  ptdMap: PostgresToDartMap
  name: string

  constructor(postgresType: PostgresType, ptdMap: PostgresToDartMap) {
    this.postgresType = postgresType
    this.ptdMap = ptdMap
    this.name = `${formatForDartClassName(this.postgresType.name)}`
  }

  generateType(): string {
    return this.name
  }

  generateDeclaration(): string {
    return `class ${this.name} implements JsonSerializable {${this.postgresType.attributes.map(
      (attr) => `
  final ${new NullDartType(this.ptdMap[attr.type_id][1]).generateType()} ${formatForDartPropertyName(attr.name)};`
    ).join('')}

  const ${this.name}({${this.postgresType.attributes.map(
    attr => {
      return `
    this.${formatForDartPropertyName(attr.name)}`
    })
    .join(',')}
  });

  static Map<String, dynamic> _generateMap({${this.postgresType.attributes
    .map(attr => {
      return `
    ${new NullDartType(this.ptdMap[attr.type_id][1]).generateType()} ${formatForDartPropertyName(attr.name)}`
    })
    .join(',')}
  }) => {${this.postgresType.attributes
    .map(attr => {
      return `
    if (${formatForDartPropertyName(attr.name)} != null) '${attr.name}': ${formatForDartPropertyName(attr.name)}${this.ptdMap[attr.type_id][1].generateJsonEncoding()}`
    })
    .join(',')}
  };

  @override
  Map<String, dynamic> toJson() => _generateMap(${this.postgresType.attributes
    .map((attr) => {
      return `
    ${formatForDartPropertyName(attr.name)}: ${formatForDartPropertyName(attr.name)}`
    })
    .join(',')}
  );

  ${this.name} copyWith({${this.postgresType.attributes
    .map((attr) => {
      return `
    ${new NullDartType(this.ptdMap[attr.type_id][1]).generateType()} ${formatForDartPropertyName(attr.name)}`
    })
    .join(',')}
  }) {
    return ${this.name}(${this.postgresType.attributes
    .map((attr) => {
      return `
      ${formatForDartPropertyName(attr.name)}: ${formatForDartPropertyName(attr.name)} ?? this.${formatForDartPropertyName(attr.name)}`
    })
    .join(',')}
    );
  }

  @override
  factory ${this.name}.fromJson(Map<String, dynamic> jsonObject) {
    return ${this.name}(${this.postgresType.attributes
      .map((attr) => {
        return `
      ${formatForDartPropertyName(attr.name)}: ${new NullDartType(this.ptdMap[attr.type_id][1]).generateJsonDecoding(`jsonObject['${attr.name}']`)}`
      })
      .join(',')}
    );
  }
}`
  }

  generateJsonEncoding(): string {
    return '.toJson()'
  }

  generateJsonDecoding(inputParameter: string): string {
    return `${this.name}.fromJson(${inputParameter})`
  }
}

class ClassDartConstruct implements Declarable {
  rowableName: string
  className: string
  operations: Operation[]
  columns: PostgresColumn[]
  ptdMap: PostgresToDartMap

  constructor(
    rowableName: string,
    operations: Operation[],
    columns: PostgresColumn[],
    ptdMap: PostgresToDartMap
  ) {
    this.rowableName = rowableName
    this.className = `${formatForDartClassName(rowableName)}Row`
    this.operations = operations
    this.columns = columns
    this.ptdMap = ptdMap
  }

  generateDeclaration(): string {
    return `class ${this.className} implements JsonSerializable {
  static const tableName = '${this.rowableName}';
  ${this.columns
      .map((column) => {
        return `
  final ${buildDartTypeFromPostgresColumn(column, this.ptdMap).generateType()} ${formatForDartPropertyName(column.name)};`
      })
      .join('')}

  const ${this.className}({${this.columns
    .map((column) => {
      return `
    ${buildDartTypeFromPostgresColumn(column, this.ptdMap) instanceof NullDartType ? '' : 'required '}this.${formatForDartPropertyName(column.name)}`
    })
    .join(',')}
  });

  static Map<String, dynamic> _generateMap({${this.columns
    .map((column) => {
      return `
    ${new NullDartType(buildDartTypeFromPostgresColumn(column, this.ptdMap)).generateType()} ${formatForDartPropertyName(column.name)}`
    })
    .join(',')}
  }) => {${this.columns
    .map((column) => {
      return `
    if (${formatForDartPropertyName(column.name)} != null) '${column.name}': ${formatForDartPropertyName(column.name)}${buildDartTypeFromPostgresColumn(column, this.ptdMap).generateJsonEncoding()}`
    })
    .join(',')}
  };

  @override
  Map<String, dynamic> toJson() => _generateMap(${this.columns
    .map((column) => {
      return `
    ${formatForDartPropertyName(column.name)}: ${formatForDartPropertyName(column.name)}`
    })
    .join(',')}
  );

  @override
  factory ${this.className}.fromJson(Map<String, dynamic> jsonObject) {
    return ${this.className}(${this.columns
      .map((column) => {
        return `
      ${formatForDartPropertyName(column.name)}: ${buildDartTypeFromPostgresColumn(column, this.ptdMap).generateJsonDecoding(`jsonObject['${column.name}']`)}`
      })
      .join(',')}
    );
  }

  ${this.className} copyWith({${this.columns
    .map((column) => {
      return `
    ${new NullDartType(buildDartTypeFromPostgresColumn(column, this.ptdMap)).generateType()} ${formatForDartPropertyName(column.name)}`
    })
    .join(',')}
  }) {
    return ${this.className}(${this.columns
    .map((column) => {
      return `
      ${formatForDartPropertyName(column.name)}: ${formatForDartPropertyName(column.name)} ?? this.${formatForDartPropertyName(column.name)}`
    })
    .join(',')}
    );
  }
${
  this.operations.indexOf('Insert') !== -1
    ? `
  static Map<String, dynamic> forInsert({${this.columns
    .map((column) => {
      return `
    ${(buildDartTypeFromPostgresColumn(column, this.ptdMap, true) instanceof NullDartType) ? '' : 'required '}${buildDartTypeFromPostgresColumn(column, this.ptdMap, true).generateType()} ${formatForDartPropertyName(column.name)}`
    })
    .join(',')}
  }) => _generateMap(${this.columns
    .map((column) => {
      return `
    ${formatForDartPropertyName(column.name)}: ${formatForDartPropertyName(column.name)}`
    })
    .join(',')}
  );
`
    : ''
}
}`
  }
}

type PostgresToDartMap = Record<number | string, [PostgresType | undefined, DartType]>

const PGTYPE_TO_DARTTYPE_MAP: Record<string, DartType> = {
  // Bool
  bool: new BuiltinDartType('bool'),

  // Numbers
  int2: new BuiltinDartType('int'),
  int4: new BuiltinDartType('int'),
  int8: new BuiltinDartType('int'),
  float4: new BuiltinDartType('double'),
  float8: new BuiltinDartType('double'),
  numeric: new BuiltinDartType('double'),

  // Time
  time: new DatetimeDartType(),
  timetz: new DatetimeDartType(),
  timestamp: new DatetimeDartType(),
  timestamptz: new DatetimeDartType(),
  date: new DatetimeDartType(),
  interval: new DurationDartType(),

  uuid: new BuiltinDartType('String'),
  text: new BuiltinDartType('String'),
  varchar: new BuiltinDartType('String'),
  jsonb: new MapDartType(new BuiltinDartType('String'), new BuiltinDartType('dynamic')),
  json: new MapDartType(new BuiltinDartType('String'), new BuiltinDartType('dynamic')),
  regclass: new BuiltinDartType('String'),
}

class ClassDartConstructForJsonschema implements DartType, Declarable {
  name: string
  props: PostgresToDartMap

  constructor(name: string, props: PostgresToDartMap) {
    this.name = `${formatForDartClassName(name)}`
    this.props = props;
  }

  generateType(): string {
    return this.name
  }

  generateDeclaration(): string {
    return `class ${this.name} implements JsonSerializable {${Object.entries(this.props).map(([name, [pgType, dartType]]) => `
  final ${dartType.generateType()} ${formatForDartPropertyName(name)};`
    ).join('')}

  const ${this.name}({${Object.entries(this.props).map(([name, [pgType, dartType]]) => {
      return `
    ${dartType instanceof NullDartType ? '' : 'required '}this.${formatForDartPropertyName(name)}`
    })
    .join(',')}
  });

  static Map<String, dynamic> _generateMap({${Object.entries(this.props).map(([name, [pgType, dartType]]) => {
      return `
    ${new NullDartType(dartType).generateType()} ${formatForDartPropertyName(name)}`
    })
    .join(',')}
  }) => {${Object.entries(this.props).map(([name, [pgType, dartType]]) => {
      return `
    if (${formatForDartPropertyName(name)} != null) '${name}': ${formatForDartPropertyName(name)}${dartType.generateJsonEncoding()}`
    })
    .join(',')}
  };

  @override
  Map<String, dynamic> toJson() => _generateMap(${Object.entries(this.props).map(([name, [pgType, dartType]]) => {
      return `
    ${formatForDartPropertyName(name)}: ${formatForDartPropertyName(name)}`
    })
    .join(',')}
  );

  @override
  factory ${this.name}.fromJson(Map<String, dynamic> jsonObject) {
    return ${this.name}(${Object.entries(this.props).map(([name, [pgType, dartType]]) => {
        return `
      ${formatForDartPropertyName(name)}: ${dartType.generateJsonDecoding(`jsonObject['${name}']`)}`
      })
      .join(',')}
    );
  }

  ${this.name} copyWith({${Object.entries(this.props).map(([name, [pgType, dartType]]) => {
      return `
    ${new NullDartType(dartType).generateType()} ${formatForDartPropertyName(name)}`
    })
    .join(',')}
  }) {
    return ${this.name}(${Object.entries(this.props).map(([name, [pgType, dartType]]) => {
      return `
      ${formatForDartPropertyName(name)}: ${formatForDartPropertyName(name)} ?? this.${formatForDartPropertyName(name)}`
    })
    .join(',')}
    );
  }
}`
  }

  generateJsonEncoding(): string {
    return '.toJson()'
  }

  generateJsonDecoding(inputParameter: string): string {
    return `${this.name}.fromJson(${inputParameter})`
  }
}

function buildDartTypeFromJsonschema(schema: any, name: string | undefined = undefined, ptdMap: PostgresToDartMap): DartType {
  if (!schema || typeof schema !== 'object') {
    return new MapDartType(new BuiltinDartType('String'), new BuiltinDartType('dynamic'));
  }

  // Handle array type
  if (schema.type === 'array' && schema.items) {
    let newTypeName = `${name}_element`
    let newType = buildDartTypeFromJsonschema(schema.items, newTypeName, ptdMap)
    ptdMap[newTypeName] = [undefined, newType]
    return new ListDartType(newType);
  }

  // Handle object type
  if (schema.type === 'object' && schema.properties) {
    if(name === undefined) {
      console.log(`No name (${name}) provided for ${JSON.stringify(schema)}`)
      return new MapDartType(new BuiltinDartType('String'), new BuiltinDartType('dynamic'));
    }
    let classProps: PostgresToDartMap = {}
    for(var p in schema.properties) {
      let newTypeName = `${p}`
      let newType = buildDartTypeFromJsonschema(schema.properties[p], newTypeName, ptdMap)
      if('required' in schema.properties && !(p in schema.properties.required)) {
        newType = new NullDartType(newType)
      }
      ptdMap[newTypeName] = [undefined, newType]
      classProps[newTypeName] = [undefined, newType]
    }
    return new ClassDartConstructForJsonschema(
      name,
      classProps
    );
  }

  // Handle primitive types
  switch (schema.type) {
    case 'string':
      return new BuiltinDartType('String');
    case 'integer':
      return new BuiltinDartType('int');
    case 'number':
      return new BuiltinDartType('double');
    case 'boolean':
      return new BuiltinDartType('bool');
    case 'null':
      return new NullDartType(new BuiltinDartType('dynamic'));
    default:
      return new BuiltinDartType('dynamic');
  }
}

function buildDartTypeFromPostgresColumn(
  postgresColumn: PostgresColumn,
  ptdMap: PostgresToDartMap,
  forInsert: boolean = false,
): DartType {
  let dartType = ptdMap[postgresColumn.format][1]
  if(postgresColumn.format === 'jsonb' && postgresColumn.name in ptdMap) {
    dartType = ptdMap[postgresColumn.name][1]
  }
  if(postgresColumn.is_nullable || (forInsert && (postgresColumn.is_generated || postgresColumn.is_identity || postgresColumn.default_value !== null))) {
    return new NullDartType(dartType)
  }
  return dartType
}

function buildDartTypeFromPostgresType(
  postgresType: PostgresType,
  ptdMap: PostgresToDartMap
): DartType {
  const sanitizedTypeName = postgresType.name.startsWith('_')
    ? postgresType.name.slice(1)
    : postgresType.name

  if (postgresType.name.startsWith('_')) {
    const existingDartType = ptdMap[sanitizedTypeName] ? ptdMap[sanitizedTypeName][1] : undefined
    if (existingDartType) {
      return new ListDartType(existingDartType)
    }
  }

  // Builtin type
  const dartTypeFromStaticMap = PGTYPE_TO_DARTTYPE_MAP[sanitizedTypeName]
  if (dartTypeFromStaticMap) {
    return postgresType.name.startsWith('_')
      ? new ListDartType(dartTypeFromStaticMap)
      : dartTypeFromStaticMap
  }

  // Enum
  if (postgresType.enums.length > 0) {
    const enumConstruct = new EnumDartConstruct(
      postgresType.name,
      postgresType.enums,
      postgresType.comment
    )
    return postgresType.name.startsWith('_') ? new ListDartType(enumConstruct) : enumConstruct
  }

  // Composite type
  if (postgresType.attributes.length > 0) {
    const compositeType = new ClassDartConstructForCompositeType(postgresType, ptdMap)
    return postgresType.name.startsWith('_') ? new ListDartType(compositeType) : compositeType
  }

  console.log(`Could not find matching type for: ${JSON.stringify(postgresType)}`)
  return new BuiltinDartType('dynamic')
}

/**
 * Sorts PostgreSQL types by their dependencies, ensuring that types referenced
 * in attributes come before the types that reference them.
 *
 * @param types Array of PostgreSQL types to sort
 * @returns Sorted array of types, or throws error if circular dependency detected
 */
export function sortTypesByDependency(types: PostgresType[]): PostgresType[] {
  interface TypeNode {
    type: PostgresType
    dependencies: Set<number>
    visited: boolean
    inStack: boolean
  }

  // Create adjacency list representation
  const typeMap = new Map<number, TypeNode>()

  // Initialize graph nodes
  for (const type of types) {
    typeMap.set(type.id, {
      type,
      dependencies: new Set(
        type.attributes.map((attr) => attr.type_id).filter((id) => id !== type.id) // Exclude self-references
      ),
      visited: false,
      inStack: false,
    })
  }

  const sorted: PostgresType[] = []

  /**
   * Performs depth-first search to detect cycles and build topological sort
   */
  function dfs(nodeId: number): void {
    const node = typeMap.get(nodeId)
    if (!node) return

    // Check for circular dependency
    if (node.inStack) {
      throw new Error(`Circular dependency detected involving type ${node.type.name}`)
    }

    // Skip if already visited in another branch
    if (node.visited) return

    // Mark node as being processed
    node.inStack = true

    // Process all dependencies first
    for (const depId of node.dependencies) {
      dfs(depId)
    }

    // Mark as visited and remove from stack
    node.visited = true
    node.inStack = false

    // Add to sorted output
    sorted.push(node.type)
  }

  // Process all nodes
  for (const nodeId of typeMap.keys()) {
    if (!typeMap.get(nodeId)!.visited) {
      dfs(nodeId)
    }
  }

  // sort the arrays for last
  return sorted.sort(({ name: a }, { name: b }) =>
    a.startsWith('_') ? 1 : b.startsWith('_') ? -1 : 0
  )
}

export function getRequiredTypes(
  allTypes: PostgresType[],
  columns: PostgresColumn[]
): PostgresType[] {
  // Create maps for quick lookups
  const typesById = new Map(allTypes.map((type) => [type.id, type]))
  const typesByName = new Map(allTypes.map((type) => [type.name, type]))

  // Get all directly referenced types from columns
  const directTypeIds = new Set<number>()

  for (const column of columns) {
    const type = typesByName.get(column.format)
    if (type) {
      directTypeIds.add(type.id)
    } else {
      console.log(
        `Type not found for column ${column.name}: format: ${column.format}\tdata_type: ${column.data_type}`
      )
    }
    const nonArrayType = column.format.startsWith('_')
      ? typesByName.get(column.format.slice(1))
      : null
    if (nonArrayType) {
      directTypeIds.add(nonArrayType.id)
    }
  }

  // Recursively collect dependent types
  const allRequiredTypeIds = new Set<number>()

  function collectDependencies(typeId: number): void {
    if (allRequiredTypeIds.has(typeId)) return

    const type = typesById.get(typeId)
    if (!type) return

    allRequiredTypeIds.add(typeId)

    for (const attr of type.attributes) {
      collectDependencies(attr.type_id)
    }
    if(type.name.startsWith('_')) {
      const nonArrayType = typesByName.get(type.name.slice(1))
      if(nonArrayType) {
        collectDependencies(nonArrayType.id)
      }
    }
  }

  // Process each direct type
  for (const typeId of directTypeIds) {
    collectDependencies(typeId)
  }

  // Get the actual type objects and sort them
  const requiredTypes = Array.from(allRequiredTypeIds).map((id) => typesById.get(id)!)

  return sortTypesByDependency(requiredTypes)
}

export const apply = ({ schemas, tables, views, columns, types }: GeneratorMetadata): string => {
  const columnsByTableId = columns
    .sort(({ name: a }, { name: b }) => a.localeCompare(b))
    .reduce(
      (acc, curr) => {
        acc[curr.table_id] ??= []
        acc[curr.table_id].push(curr)
        return acc
      },
      {} as Record<string, PostgresColumn[]>
    )

  let declarableTypes: Declarable[] = []
  const requiredTypes = getRequiredTypes(types, columns)
  let ptdMap: PostgresToDartMap = {}
  for (const t of requiredTypes) {
    const newDartType = buildDartTypeFromPostgresType(t, ptdMap)
    ptdMap[t.id] = [t, newDartType]
    ptdMap[t.name] = [t, newDartType]
    if(newDartType instanceof EnumDartConstruct || newDartType instanceof ClassDartConstructForCompositeType) {
      declarableTypes.push(newDartType)
    }
  }

  for(const c of columns) {
    if(c.format === 'jsonb' && c.check && c.check.startsWith('jsonb_matches_schema')) {
      let jsonschemaDartType = buildDartTypeFromJsonschema(JSON.parse(c.check.substring('jsonb_matches_schema(\''.length, c.check.lastIndexOf('}')+1)), c.name, ptdMap)
      if(c.is_nullable) jsonschemaDartType = new NullDartType(jsonschemaDartType)
      ptdMap[c.name] = [undefined, jsonschemaDartType]
    }
  }
  for(const k in ptdMap) {
    const t = ptdMap[k][1]
    if(t instanceof ClassDartConstructForJsonschema) {
      declarableTypes.push(t)
    }
  }

  const tableClassConstructs = tables
    .filter((table) => schemas.some((schema) => schema.name === table.schema))
    .map(
      (table) =>
        new ClassDartConstruct(
          table.name,
          ['Select', 'Insert', 'Update'],
          columnsByTableId[table.id],
          ptdMap
        )
    )

  const viewClassConstructs = views
    .filter((view) => schemas.some((schema) => schema.name === view.schema))
    .map((view) => new ClassDartConstruct(view.name, ['Select'], columnsByTableId[view.id], ptdMap))

  let result = `
Duration parsePostgresInterval(String interval) {
  // Regular expression to match HH:MM:SS format
  final regex = RegExp(r'^([0-9]{2}):([0-5][0-9]):([0-5][0-9])$');
  final match = regex.firstMatch(interval);
  
  if (match == null) {
    throw FormatException('Invalid interval format. Expected HH:MM:SS');
  }

  final hours = int.parse(match.group(1)!);
  final minutes = int.parse(match.group(2)!);
  final seconds = int.parse(match.group(3)!);

  return Duration(
    hours: hours,
    minutes: minutes,
    seconds: seconds,
  );
}

abstract class JsonSerializable {
  Map<String, dynamic> toJson();

  // We can't declare a constructor in an interface, but we can declare
  // a factory constructor that implementing classes must provide
  factory JsonSerializable.fromJson(Map<String, dynamic> json) {
    throw UnimplementedError();
  }
}

${declarableTypes
  .map((t) => t.generateDeclaration())
  .join('\n\n')}

${tableClassConstructs.map((classConstruct) => classConstruct.generateDeclaration()).join('\n\n')}

${viewClassConstructs.map((classConstruct) => classConstruct.generateDeclaration()).join('\n\n')}
`
  return result
}
