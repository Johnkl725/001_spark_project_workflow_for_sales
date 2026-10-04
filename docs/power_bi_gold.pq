// Crear un parametro de texto pGoldFolder con la ruta Windows de exports\gold.
// Pegar este archivo en una consulta en blanco llamada fxGoldTable.
(tableName as text) as table =>
let
    Root = Text.TrimEnd(pGoldFolder, {"\", "/"}),
    Manifest = Json.Document(File.Contents(Root & "\current.json")),
    Entry = Record.Field(Manifest[tables], tableName),
    RelativePath = Entry[csv_relative_path],
    Files = Folder.Files(Root & "\" & Text.Replace(RelativePath, "/", "\")),
    Parts = Table.SelectRows(Files, each
        Text.StartsWith([Name], "part-") and Text.EndsWith([Name], ".csv")
        and [Attributes]?[Hidden]? <> true),
    Schema = Entry[schema],
    Names = List.Transform(Schema, each _[name]),
    TypeOf = (sparkType as text) as type =>
        if Text.StartsWith(sparkType, "decimal(") then Currency.Type
        else if List.Contains({"long", "bigint", "integer", "int", "short"}, sparkType) then Int64.Type
        else if sparkType = "date" then type date
        else if sparkType = "boolean" then type logical
        else if sparkType = "string" then type text
        else error "Tipo Gold no soportado: " & sparkType,
    Types = List.Transform(Schema, each {_[name], TypeOf(_[type])}),
    ReadPart = (content as binary) as table =>
        let
            Csv = Csv.Document(content, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
            Headers = Table.PromoteHeaders(Csv, [PromoteAllScalars=true]),
            Checked = if List.Sort(Table.ColumnNames(Headers)) = List.Sort(Names)
                      then Table.ReorderColumns(Headers, Names)
                      else error "El encabezado CSV no coincide con el contrato Gold"
        in Checked,
    Combined = if Table.IsEmpty(Parts)
               then error "No hay archivos part-*.csv para " & tableName
               else Table.Combine(List.Transform(Parts[Content], ReadPart)),
    Typed = Table.TransformColumnTypes(Combined, Types, "en-US")
in
    Typed
