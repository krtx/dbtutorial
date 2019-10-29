import 'dart:typed_data';
import 'dart:io';
import 'dart:convert';

/// 各フィールドのサイズ
const IdSize = 4;
const UsernameSize = 32;
const EmailSize = 255;

/// オフセット
const IdOffset = 0;
const UsernameOffset = IdOffset + IdSize;
const EmailOffset = UsernameOffset + UsernameSize;

/// rowのサイズ
const RowSize = IdSize + UsernameSize + EmailSize;

/// 1ページのサイズ
const PageSize = 4096;

/// ページの総数
const TableMaxPages = 100;

/// 1ページあたりのrowの数
const RowsPerPage = PageSize / RowSize;

/// 最大row数
const TableMaxRows = RowsPerPage * TableMaxPages;

class Row {
  int id;
  Uint8List username;
  Uint8List email;

  Row.empty() {}

  Row(int id, String username, String email) {
    this.id = id;

    var encoder = Utf8Encoder();

    this.username = encoder.convert(username);
    this.email = encoder.convert(email);
  }

  String toString() {
    var decoder = Utf8Decoder();
    return "(${this.id},${decoder.convert(this.username)},${decoder.convert(this.email)})";
  }
}

class ExecuteTableFullException implements Exception {}

class Table {
  int rows;
  List<Uint8List> pages;

  Table() {
    rows = 0;
    pages = new List(TableMaxPages);
  }

  void add(Row row) {
    if (this.rows >= TableMaxRows) {
      throw ExecuteTableFullException();
    }

    int pageIndex = this.rows ~/ RowsPerPage;
    if (this.pages[pageIndex] == null) {
      this.pages[pageIndex] = Uint8List(PageSize);
    }

    int rowOffset = this.rows.remainder(RowsPerPage).toInt();
    int byteOffset = rowOffset * RowSize;

    // serialize
    this.pages[pageIndex][byteOffset] = (row.id >> 24) & 255;
    this.pages[pageIndex][byteOffset + 1] = (row.id >> 16) & 255;
    this.pages[pageIndex][byteOffset + 2] = (row.id >> 8) & 255;
    this.pages[pageIndex][byteOffset + 3] = row.id & 255;
    for (var i = 0; i < row.username.length; i++) {
      this.pages[pageIndex][i + IdSize + byteOffset] = row.username[i];
    }
    for (var i = 0; i < row.email.length; i++) {
      this.pages[pageIndex][i + IdSize + UsernameSize + byteOffset] = row.email[i];
    }

    this.rows += 1;
  }

  List<Row> select() {
    List<Row> rows = [];

    // deserialize
    for (int i = 0; i < this.rows; i++) {
      Row row = Row.empty();

      int pageIndex = i ~/ RowsPerPage;
      int rowOffset = i.remainder(RowsPerPage).toInt();
      int byteOffset = rowOffset * RowSize;

      row.id = (this.pages[pageIndex][byteOffset] << 24) |
          (this.pages[pageIndex][byteOffset + 1] << 16) |
          (this.pages[pageIndex][byteOffset + 2] << 8) |
          (this.pages[pageIndex][byteOffset + 3]);

      row.username = Uint8List.fromList(this.pages[pageIndex].skip(byteOffset + IdSize).take(UsernameSize).toList());
      row.email =
          Uint8List.fromList(this.pages[pageIndex].skip(byteOffset + IdSize + UsernameSize).take(EmailSize).toList());

      rows.add(row);
    }

    return rows;
  }
}

enum StatementType { insert, select }

class StatementInvalidArgumentsException implements Exception {}

class StatementUnrecognizedException implements Exception {}

class Statement {
  StatementType type;

  Row rowToInsert;

  Statement.prepare(String line) {
    if (line.startsWith("insert")) {
      type = StatementType.insert;
      var args = line.split(" ");

      if (args.length != 4) {
        throw StatementInvalidArgumentsException();
      }

      var id = int.tryParse(args[1]);
      if (id == null) {
        throw StatementInvalidArgumentsException();
      }

      rowToInsert = Row(id, args[2], args[3]);
    } else if (line.startsWith("select")) {
      type = StatementType.select;
    } else {
      throw StatementUnrecognizedException();
    }
  }

  execute(Table table) {
    switch (type) {
      case StatementType.insert:
        table.add(this.rowToInsert);
        break;
      case StatementType.select:
        print(table.select());
        break;
      default:
        throw Error();
    }
  }
}

void printPrompt() {
  stdout.write("db > ");
}

void main() {
  Table table = Table();

  while (true) {
    printPrompt();
    String line = stdin.readLineSync();

    if (line == null) {
      exitCode = 0;
      break;
    }

    if (line.startsWith(".")) {
      if (line == '.exit') {
        exitCode = 0;
        break;
      } else {
        stdout.writeln("Unrecognized command: '${line}'.");
      }
      continue;
    }

    try {
      Statement statement = Statement.prepare(line);
      statement.execute(table);
    } on StatementUnrecognizedException catch (_) {
      stderr.writeln("Unrecognized command: '${line}'");
    }
  }
}
