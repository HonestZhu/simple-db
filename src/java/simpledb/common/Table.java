package simpledb.common;

import simpledb.storage.DbFile;

import java.util.Objects;

public class Table {
    private DbFile file;
    private String name;
    private String pkeyField;

    public Table(DbFile file, String name, String pkeyField) {
        this.file = file;
        this.name = name;
        this.pkeyField = pkeyField;
    }

    public DbFile getFile() {
        return file;
    }

    public void setFile(DbFile file) {
        this.file = file;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPkeyField() {
        return pkeyField;
    }

    public void setPkeyField(String pkeyField) {
        this.pkeyField = pkeyField;
    }

    public Table(DbFile file, String name) {
        new Table(file, name, "");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Table table = (Table) o;

        if (!Objects.equals(file, table.file)) return false;
        if (!Objects.equals(name, table.name)) return false;
        return Objects.equals(pkeyField, table.pkeyField);
    }

    @Override
    public int hashCode() {
        int result = file != null ? file.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (pkeyField != null ? pkeyField.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Table{" +
                "file=" + file +
                ", name='" + name + '\'' +
                ", pkeyField='" + pkeyField + '\'' +
                '}';
    }
}
