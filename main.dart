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

    int pageIndex = this.numRows ~/ Pager.RowsPerPage;
    int rowOffset = this.numRows.remainder(Pager.RowsPerPage).toInt();
    int byteOffset = rowOffset * Row.Size;
    row.serialize(this.pager.fetch(pageIndex), byteOffset);

    this.numRows += 1;
  }

  List<Row> select() {
    List<Row> rows = [];

    for (int i = 0; i < this.numRows; i++) {
      int pageIndex = i ~/ Pager.RowsPerPage;
      int rowOffset = i.remainder(Pager.RowsPerPage).toInt();
      int byteOffset = rowOffset * Row.Size;
      Row row = Row.deserialize(this.pager.fetch(pageIndex), byteOffset);
      rows.add(row);
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
