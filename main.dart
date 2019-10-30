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

  /// deserialize the data from the buffer
  Row.deserialize(Uint8List buffer) {
    assert(buffer.length >= Size);

    // id (big endian)
    this.id = (buffer[0] << 24) | (buffer[1] << 16) | (buffer[2] << 8) | buffer[3];

    // username
    this.username = Uint8List.fromList(buffer.skip(IdSize).take(UsernameSize).toList());

    // email
    this.email = Uint8List.fromList(buffer.skip(IdSize + UsernameSize).take(EmailSize).toList());
  }

  /// serialize the data into the buffer
  /// the length of serialized list is fixed (RowSize = IdSize + UsernameSize + EmailSize)
  void serialize(Uint8List buffer) {
    assert(buffer.length >= Size);

    // id (big endian)
    buffer[0] = (this.id >> 24) & 255;
    buffer[1] = (this.id >> 16) & 255;
    buffer[2] = (this.id >> 8) & 255;
    buffer[3] = this.id & 255;

    // username
    for (var i = 0; i < this.username.length; i++) {
      buffer[IdSize + i] = this.username[i];
    }

    // email
    for (var i = 0; i < this.email.length; i++) {
      buffer[IdSize + UsernameSize + i] = this.email[i];
    }
  }

  String toString() {
    var decoder = Utf8Decoder();
    return "(${this.id}, ${decoder.convert(this.username)}, ${decoder.convert(this.email)})";
  }
}

class Pager {
  RandomAccessFile file;
  List<Uint8List> pages;

  static const MaxPages = 100;
  static const PageSize = 4096;
  static const RowsPerPage = PageSize ~/ Row.Size;

  Pager.open(String filename) {
    this.file = File(filename).openSync(mode: FileMode.append);
    this.pages = List(MaxPages);
  }

  Uint8List fetch(int pageIndex) {
    assert(pageIndex < MaxPages);

    if (this.pages[pageIndex] == null) {
      Uint8List page;
      var numPages = (this.file.lengthSync() + PageSize - 1) ~/ PageSize;
      if (pageIndex < numPages) {
        this.file.setPositionSync(pageIndex * PageSize);
        page = this.file.readSync(PageSize);
      } else {
        page = Uint8List(PageSize);
      }
      this.pages[pageIndex] = page;
    }

    return this.pages[pageIndex];
  }

  void flush(int pageIndex, {int size = PageSize}) {
    if (this.pages[pageIndex] == null) {
      return;
    }

    this.file.setPositionSync(pageIndex * PageSize);
    this.file.writeFromSync(this.pages[pageIndex], 0, size);
  }
}

/// Cursor object represents a location in the table
class Cursor {
  Table table;

  /// current row index
  int rowIndex;

  /// Indicates a position one past the last element
  bool endOfTable;

  /// Cursor object points to the start of the table, where a row may exist
  Cursor.tableStart(this.table) {
    this.rowIndex = 0;
    this.endOfTable = this.table.numRows == 0;
  }

  /// Cursor object points to the end of the table (not inclusive)
  /// This cursor always points to the place where new rows should be added
  Cursor.tableEnd(this.table) {
    this.rowIndex = this.table.numRows;
    this.endOfTable = true;
  }

  /// increments the pointer position by 1, even if the pointer is in the end of the table
  advance() {
    this.rowIndex += 1;
    if (this.rowIndex >= this.table.numRows) {
      this.endOfTable = true;
    }
  }

  /// the pointer value
  Uint8List value() {
    int pageIndex = this.rowIndex ~/ Pager.RowsPerPage;
    int rowOffset = this.rowIndex.remainder(Pager.RowsPerPage).toInt();
    int byteOffset = rowOffset * Row.Size;

    // this Uint8List is a view of the underlying page and changes in this list
    // will be visible in the original list and vice versa
    return Uint8List.view(this.table.pager.fetch(pageIndex).buffer, byteOffset);
  }
}

class Table {
  int numRows;
  Pager pager;

  static const TableMaxRows = Pager.RowsPerPage * Pager.MaxPages;

  Table.open(String filename) {
    this.pager = Pager.open(filename);
    this.numRows = this.pager.file.lengthSync() ~/ Row.Size;
  }

  void add(Row row) {
    if (this.numRows >= TableMaxRows) {
      throw 'the table is full';
    }

    // serialize the row into the end of the table
    row.serialize(Cursor.tableEnd(this).value());

    this.numRows += 1;
  }

  List<Row> select() {
    Cursor cursor = Cursor.tableStart(this);
    List<Row> rows = [];
    while (!(cursor.endOfTable)) {
      rows.add(Row.deserialize(cursor.value()));
      cursor.advance();
    }

    return rows;
  }

  void close() {
    var numFullPages = this.numRows ~/ Pager.RowsPerPage;
    for (var i = 0; i < numFullPages; i++) {
      this.pager.flush(i);
    }

    var numAdditionalRows = this.numRows.remainder(Pager.RowsPerPage);
    if (numAdditionalRows > 0) {
      this.pager.flush(numFullPages, size: numAdditionalRows * Row.Size);
    }
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
        stdout.writeln('Executed.');
        break;
      case StatementType.select:
        var rows = table.select();
        for (var row in rows) {
          print(row);
        }
        break;
    }
  }
}

void main(List<String> arguments) {
  if (arguments.length != 1) {
    stderr.writeln('Must supply a database filename.');
    exitCode = 1;
    return;
  }

  Table table = Table.open(arguments[0]);

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
        table.close();
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
