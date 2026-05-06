Attribute VB_Name = "Module1"
Option Explicit

#If Not Mac Then
Private Type SYSTEMTIME
    wYear As Integer
    wMonth As Integer
    wDayOfWeek As Integer
    wDay As Integer
    wHour As Integer
    wMinute As Integer
    wSecond As Integer
    wMilliseconds As Integer
End Type

Private Type TIME_ZONE_INFORMATION
    Bias As Long
    StandardName(0 To 31) As Integer
    StandardDate As SYSTEMTIME
    StandardBias As Long
    DaylightName(0 To 31) As Integer
    DaylightDate As SYSTEMTIME
    DaylightBias As Long
End Type

#If VBA7 Then
Private Declare PtrSafe Function GetTimeZoneInformation Lib "kernel32" (ByRef lpTimeZoneInformation As TIME_ZONE_INFORMATION) As Long
#Else
Private Declare Function GetTimeZoneInformation Lib "kernel32" (ByRef lpTimeZoneInformation As TIME_ZONE_INFORMATION) As Long
#End If

Private Const TIME_ZONE_ID_STANDARD As Long = 1
Private Const TIME_ZONE_ID_DAYLIGHT As Long = 2
#End If

Sub say_hello()
    ExportReviewJson
End Sub

Sub ExportReviewJson()
    Const FIRST_DATA_ROW As Long = 6
    Const COL_DECISION As Long = 9
    Const COL_FALSE_REASON As Long = 11
    Const COL_ACTION_PLAN As Long = 12
    Const COL_ACTION_DUE_DATE As Long = 13
    Const COL_SCAN_PATH As Long = 14
    Const COL_FINDING_KEY As Long = 15
    Const COL_FINDING_HASH As Long = 16
    Const COL_FILE_IDENTIFIER As Long = 17
    Const COL_HIVE_DATABASE As Long = 18
    Const COL_HIVE_TABLE As Long = 19
    Const COL_HIVE_TABLE_FQN As Long = 20
    Const COL_COLUMN_NAME As Long = 21
    Const COL_PII_TYPE As Long = 22
    Const COL_SAMPLE_ROW_COUNT As Long = 23
    Const COL_MATCH_COUNT As Long = 24
    Const COL_NON_EMPTY_RATIO As Long = 25

    Dim ws As Worksheet
    Set ws = ThisWorkbook.Worksheets("review")

    Dim responder As String
    responder = TrimText(ws.Cells(3, 2).Value)
    If responder = "" Then
        MsgBox "Responder is required.", vbExclamation
        ws.Cells(3, 2).Select
        Exit Sub
    End If

    Dim lastRow As Long
    lastRow = ws.Cells(ws.Rows.Count, COL_FINDING_KEY).End(xlUp).Row
    If lastRow < FIRST_DATA_ROW Then
        MsgBox "No findings to export.", vbExclamation
        Exit Sub
    End If

    ClearValidationMarks ws, FIRST_DATA_ROW, lastRow

    Dim responses As String
    responses = ""

    Dim rowIndex As Long
    Dim errorCell As Range
    Dim errorCount As Long
    For rowIndex = FIRST_DATA_ROW To lastRow
        If TrimText(ws.Cells(rowIndex, COL_FINDING_KEY).Value) <> "" Then
            Dim decision As String
            decision = NormalizeDecision(ws.Cells(rowIndex, COL_DECISION).Value)
            If decision = "" Then
                MarkInvalid ws.Cells(rowIndex, COL_DECISION), errorCell, errorCount
            ElseIf decision = "false_positive" Then
                If TrimText(ws.Cells(rowIndex, COL_FALSE_REASON).Value) = "" Then
                    MarkInvalid ws.Cells(rowIndex, COL_FALSE_REASON), errorCell, errorCount
                End If
            ElseIf decision = "true_positive" Then
                If TrimText(ws.Cells(rowIndex, COL_ACTION_PLAN).Value) = "" Then
                    MarkInvalid ws.Cells(rowIndex, COL_ACTION_PLAN), errorCell, errorCount
                End If
                If Not IsActionDueDateWithinWindow(ws.Cells(rowIndex, COL_ACTION_DUE_DATE)) Then
                    MarkInvalid ws.Cells(rowIndex, COL_ACTION_DUE_DATE), errorCell, errorCount
                End If
            Else
                MarkInvalid ws.Cells(rowIndex, COL_DECISION), errorCell, errorCount
            End If

            If errorCount = 0 Then
                If responses <> "" Then responses = responses & ","
                responses = responses & ResponseJson(ws, rowIndex, decision)
            End If
        End If
    Next rowIndex

    If errorCount > 0 Then
        MsgBox "Required review cells are missing or invalid.", vbExclamation
        errorCell.Select
        Exit Sub
    End If

    Dim scanPath As String
    scanPath = ReadMetadata("scan_path")
    If scanPath = "" Then scanPath = TrimText(ws.Cells(FIRST_DATA_ROW, COL_SCAN_PATH).Value)

    Dim respondedAt As String
    respondedAt = RespondedAtIso()
    If respondedAt = "" Then
        MsgBox "Unable to determine timezone offset. review.json was not created.", vbExclamation
        Exit Sub
    End If

    Dim json As String
    json = "{""schema_version"":1,""scan_path"":" & JsonString(scanPath) & _
        ",""responder"":" & JsonString(responder) & _
        ",""responded_at"":" & JsonString(respondedAt) & _
        ",""responses"":[" & responses & "]}"

    Dim targetPath As Variant
    targetPath = Application.GetSaveAsFilename(InitialFileName:="review.json", FileFilter:="JSON Files (*.json), *.json")
    If VarType(targetPath) = vbBoolean Then Exit Sub

    WriteUtf8File CStr(targetPath), json
    MsgBox "review.json has been created.", vbInformation
End Sub

Private Function ResponseJson(ws As Worksheet, rowIndex As Long, decision As String) As String
    Dim result As String
    result = "{""finding_key"":" & JsonString(ws.Cells(rowIndex, 15).Value) & _
        ",""finding_hash"":" & JsonString(ws.Cells(rowIndex, 16).Value) & _
        ",""file_identifier"":" & JsonString(ws.Cells(rowIndex, 17).Value) & _
        ",""file_identifier_pattern"":" & JsonString(FileIdentifierPattern(ws, rowIndex)) & _
        ",""hive_database"":" & JsonString(ws.Cells(rowIndex, 18).Value) & _
        ",""hive_table"":" & JsonString(ws.Cells(rowIndex, 19).Value) & _
        ",""hive_table_fqn"":" & JsonString(ws.Cells(rowIndex, 20).Value) & _
        ",""column_name"":" & JsonString(ws.Cells(rowIndex, 21).Value) & _
        ",""pii_type"":" & JsonString(ws.Cells(rowIndex, 22).Value) & _
        ",""sample_row_count"":" & NumericText(ws.Cells(rowIndex, 23).Value, "0") & _
        ",""match_count"":" & NumericText(ws.Cells(rowIndex, 24).Value, "0") & _
        ",""non_empty_match_ratio"":" & NumericText(ws.Cells(rowIndex, 25).Value, "0") & _
        ",""decision"":" & JsonString(decision)
    If decision = "false_positive" Then
        result = result & ",""false_positive_reason"":" & JsonString(ws.Cells(rowIndex, 11).Value) & _
            ",""allowlist_scope"":""recurring"",""expires_at"":""9999-12-31"""
    Else
        result = result & ",""action_plan"":" & JsonString(ws.Cells(rowIndex, 12).Value) & _
            ",""action_due_date"":" & JsonString(CellDateIso(ws.Cells(rowIndex, 13)))
    End If
    ResponseJson = result & "}"
End Function

Private Function FileIdentifierPattern(ws As Worksheet, rowIndex As Long) As String
    If TrimText(ws.Cells(rowIndex, 20).Value) = "" Then
        FileIdentifierPattern = TrimText(ws.Cells(rowIndex, 17).Value)
    Else
        FileIdentifierPattern = ""
    End If
End Function

Private Function JsonString(value As Variant) As String
    Dim text As String
    text = CStr(value)
    text = Replace(text, "\", "\\")
    text = Replace(text, """", "\""")
    text = Replace(text, vbCrLf, "\n")
    text = Replace(text, vbCr, "\n")
    text = Replace(text, vbLf, "\n")
    JsonString = """" & text & """"
End Function

Private Function NormalizeDecision(value As Variant) As String
    Dim text As String
    text = LCase$(TrimText(value))
    If text = "false_positive" Or text = "fp" Then
        NormalizeDecision = "false_positive"
    ElseIf text = "true_positive" Or text = "tp" Then
        NormalizeDecision = "true_positive"
    ElseIf text = ChrW(&HC624) & ChrW(&HD0D0) Then
        NormalizeDecision = "false_positive"
    ElseIf text = ChrW(&HC815) & ChrW(&HD0D0) Then
        NormalizeDecision = "true_positive"
    Else
        NormalizeDecision = text
    End If
End Function

Private Function IsActionDueDateWithinWindow(cell As Range) As Boolean
    Dim isoDate As String
    isoDate = CellDateIso(cell)
    If isoDate = "" Then
        IsActionDueDateWithinWindow = False
        Exit Function
    End If

    Dim dueDate As Date
    dueDate = DateSerial(CInt(Left$(isoDate, 4)), CInt(Mid$(isoDate, 6, 2)), CInt(Right$(isoDate, 2)))
    IsActionDueDateWithinWindow = dueDate >= Date And dueDate <= DateAdd("d", 30, Date)
End Function

Private Function CellDateIso(cell As Range) As String
    Dim text As String
    text = TrimText(cell.Text)
    If IsIsoDateOnly(text) Then
        CellDateIso = text
        Exit Function
    End If

    text = TrimText(cell.Value)
    If IsIsoDateOnly(text) Then
        CellDateIso = text
        Exit Function
    End If

    If IsDate(cell.Value) Then
        CellDateIso = Format$(CDate(cell.Value), "yyyy-mm-dd")
    Else
        CellDateIso = ""
    End If
End Function

Private Function IsIsoDateOnly(value As String) As Boolean
    If Not value Like "####-##-##" Then
        IsIsoDateOnly = False
        Exit Function
    End If

    On Error GoTo InvalidDate
    Dim yearPart As Integer
    Dim monthPart As Integer
    Dim dayPart As Integer
    Dim parsed As Date
    yearPart = CInt(Left$(value, 4))
    monthPart = CInt(Mid$(value, 6, 2))
    dayPart = CInt(Right$(value, 2))
    parsed = DateSerial(yearPart, monthPart, dayPart)
    IsIsoDateOnly = Year(parsed) = yearPart And Month(parsed) = monthPart And Day(parsed) = dayPart
    Exit Function
InvalidDate:
    IsIsoDateOnly = False
End Function

Private Function NumericText(value As Variant, fallback As String) As String
    Dim text As String
    text = TrimText(value)
    If text = "" Then
        NumericText = fallback
    Else
        NumericText = Replace(text, ",", "")
    End If
End Function

Private Function TrimText(value As Variant) As String
    TrimText = Trim$(CStr(value))
End Function

Private Function ReadMetadata(key As String) As String
    On Error GoTo Missing
    Dim ws As Worksheet
    Set ws = ThisWorkbook.Worksheets("_metadata")
    Dim rowIndex As Long
    For rowIndex = 1 To ws.Cells(ws.Rows.Count, 1).End(xlUp).Row
        If CStr(ws.Cells(rowIndex, 1).Value) = key Then
            ReadMetadata = CStr(ws.Cells(rowIndex, 2).Value)
            Exit Function
        End If
    Next rowIndex
Missing:
    ReadMetadata = ""
End Function

Private Function RespondedAtIso() As String
    Dim offsetText As String
    offsetText = TimeZoneOffsetIso()
    If offsetText = "" Then
        RespondedAtIso = ""
    Else
        RespondedAtIso = Format$(Now, "yyyy-mm-dd\Thh:nn:ss") & offsetText
    End If
End Function

Private Function TimeZoneOffsetIso() As String
#If Mac Then
    TimeZoneOffsetIso = ""
#Else
    On Error GoTo UnknownOffset
    Dim info As TIME_ZONE_INFORMATION
    Dim status As Long
    Dim biasMinutes As Long
    status = GetTimeZoneInformation(info)
    biasMinutes = info.Bias
    If status = TIME_ZONE_ID_STANDARD Then
        biasMinutes = biasMinutes + info.StandardBias
    ElseIf status = TIME_ZONE_ID_DAYLIGHT Then
        biasMinutes = biasMinutes + info.DaylightBias
    End If
    TimeZoneOffsetIso = FormatOffsetMinutes(-biasMinutes)
    Exit Function
UnknownOffset:
    TimeZoneOffsetIso = ""
#End If
End Function

Private Function FormatOffsetMinutes(offsetMinutes As Long) As String
    Dim sign As String
    Dim absoluteMinutes As Long
    If offsetMinutes < 0 Then
        sign = "-"
        absoluteMinutes = -offsetMinutes
    Else
        sign = "+"
        absoluteMinutes = offsetMinutes
    End If
    FormatOffsetMinutes = sign & Right$("0" & CStr(absoluteMinutes \ 60), 2) & ":" & Right$("0" & CStr(absoluteMinutes Mod 60), 2)
End Function

Private Sub ClearValidationMarks(ws As Worksheet, firstRow As Long, lastRow As Long)
    ws.Range(ws.Cells(firstRow, 9), ws.Cells(lastRow, 13)).Interior.Pattern = xlNone
End Sub

Private Sub MarkInvalid(cell As Range, ByRef firstError As Range, ByRef errorCount As Long)
    errorCount = errorCount + 1
    cell.Interior.Color = RGB(255, 230, 230)
    If firstError Is Nothing Then Set firstError = cell
End Sub

Private Sub WriteUtf8File(path As String, content As String)
    On Error GoTo Fallback
    Dim textStream As Object
    Dim binaryStream As Object
    Set textStream = CreateObject("ADODB.Stream")
    textStream.Type = 2
    textStream.Charset = "utf-8"
    textStream.Open
    textStream.WriteText content
    textStream.Position = 3
    Set binaryStream = CreateObject("ADODB.Stream")
    binaryStream.Type = 1
    binaryStream.Open
    textStream.CopyTo binaryStream
    binaryStream.SaveToFile path, 2
    binaryStream.Close
    textStream.Close
    Exit Sub
Fallback:
    Dim fileNo As Integer
    fileNo = FreeFile
    Open path For Output As #fileNo
    Print #fileNo, content
    Close #fileNo
End Sub
