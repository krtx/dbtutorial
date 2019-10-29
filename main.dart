import 'dart:typed_data';
import 'dart:io';
import 'dart:convert';

/// a row of the table
class Row {
  int id;
  Uint8List username;
  Uint8List email;

  static const IdSize = 4;
  static const UsernameSize = 32;
  static const EmailSize = 255;
  static const Size = IdSize + UsernameSize + EmailSize;

  Row(int id, String username, String email) {
    var encoder = Utf8Encoder();
    this.id = id;
    this.username = encoder.convert(username);
    this.email = encoder.convert(email);
  }

  /// deserialize the data from the buffer starting from offset
  Row.deserialize(Uint8List buffer, int offset) {
    assert(buffer.length >= offset + Size);

    // id (big endian)
    this.id = (buffer[offset] << 24) | (buffer[offset + 1] << 16) | (buffer[offset + 2] << 8) | buffer[offset + 3];

    // username
    this.username = Uint8List.fromList(buffer.skip(offset + IdSize).take(UsernameSize).toList());

    // email
    this.email = Uint8List.fromList(buffer.skip(offset + IdSize + UsernameSize).take(EmailSize).toList());
  }

  /// serialize the data into the buffer starting from offset
  /// the length of serialized list is fixed (RowSize = IdSize + UsernameSize + EmailSize)
  void serialize(Uint8List buffer, int offset) {
    assert(buffer.length >= offset + Size);

    // id (big endian)
    buffer[offset] = (this.id >> 24) & 255;
    buffer[offset + 1] = (this.id >> 16) & 255;
    buffer[offset + 2] = (this.id >> 8) & 255;
    buffer[offset + 3] = this.id & 255;

    // username
    for (var i = 0; i < this.username.length; i++) {
      buffer[offset + IdSize + i] = this.username[i];
    }

    // email
    for (var i = 0; i < this.email.length; i++) {
      buffer[offset + IdSize + UsernameSize + i] = this.email[i];
    }
  }

  String toString() {
    var decoder = Utf8Decoder();
    return "(${this.id},${decoder.convert(this.username)},${decoder.convert(this.email)})";
  }
}

class Table {
  int numRows;
  List<Uint8List> pages;

  static const PageSize = 4096;
  static const MaxPages = 100;
  static const RowsPerPage = PageSize / Row.Size;
  static const TableMaxRows = RowsPerPage * MaxPages;

  Table() {
    numRows = 0;
    pages = new List(MaxPages);
  }

  void add(Row row) {
    if (this.numRows >= TableMaxRows) {
      throw 'the table is full';
    }

    int pageIndex = this.numRows ~/ RowsPerPage;
    if (this.pages[pageIndex] == null) {
      this.pages[pageIndex] = Uint8List(PageSize);
    }

    int rowOffset = this.numRows.remainder(RowsPerPage).toInt();
    int byteOffset = rowOffset * Row.Size;

    row.serialize(this.pages[pageIndex], byteOffset);

    this.numRows += 1;
  }

  List<Row> select() {
    List<Row> rows = [];

    // deserialize
    for (int i = 0; i < this.numRows; i++) {
      int pageIndex = i ~/ RowsPerPage;
      int rowOffset = i.remainder(RowsPerPage).toInt();
      int byteOffset = rowOffset * Row.Size;

      Row row = Row.deserialize(this.pages[pageIndex], byteOffset);
      rows.add(row);
    }

    return rows;
  }
}

enum StatementType { insert, select }

class Statement {
  /// which type the statement is
  StatementType type;

  /// the row to be inserted to table. this variable has a value when the statement is insert
  Row rowToInsert;

  /// parse the user input and prepare for execution
  Statement.prepare(String line) {
    if (line.startsWith('insert')) {
      type = StatementType.insert;
      var args = line.split(' ');

      if (args.length != 4) {
        throw 'insert statement should have 3 arguments';
      }

      var id = int.tryParse(args[1]);
      if (id == null) {
        throw 'the first argument of insert should be an integer';
      }

      rowToInsert = Row(id, args[2], args[3]);
    } else if (line.startsWith('select')) {
      type = StatementType.select;
    } else {
      throw "unrecognized command: '${line}'";
    }
  }

  /// execute the prepared statement
  execute(Table table) {
    switch (type) {
      case StatementType.insert:
        table.add(this.rowToInsert);
        break;
      case StatementType.select:
        print(table.select());
        break;
    }
  }
}

void main() {
  Table table = Table();

  // repl
  while (true) {
    // prompt
    stdout.write("db > ");

    // read
    String line = stdin.readLineSync();
    if (line == null) {
      exitCode = 0;
      break;
    }

    // meta commands
    if (line.startsWith(".")) {
      if (line == '.exit') {
        exitCode = 0;
        break;
      } else {
        stdout.writeln("unrecognized command: '${line}'.");
      }
      continue;
    }

    Statement statement;
    try {
      statement = Statement.prepare(line);
    } catch (e) {
      stderr.writeln("to parse the statement failed: ${e}");
      continue;
    }

    try {
      statement.execute(table);
    } catch (e) {
      stderr.writeln("to execute the statement failed: ${e}");
    }
  }
}
