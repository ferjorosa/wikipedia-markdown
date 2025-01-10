<table>
  <caption>Outer Table with Rowspan and Colspan</caption>
  <tr>
    <th>Header 1</th>
    <th>Header 2</th>
    <th>Header 3</th>
  </tr>
  <tr>
    <td rowspan="2">Row 1 and 2, Cell 1</td>
    <td colspan="2">Row 1, Cell 2 and 3</td>
  </tr>
  <tr>
    <td>Row 2, Cell 2</td>
    <td>
      <table>
        <caption>Inner Table</caption>
        <tr>
          <td>Inner Cell 1</td>
          <td>Inner Cell 2</td>
        </tr>
        <tr>
          <td colspan="2">Inner Cell Spanning 2 Columns</td>
        </tr>
      </table>
    </td>
  </tr>
  <tr>
    <td>Row 3, Cell 1</td>
    <td>Row 3, Cell 2</td>
    <td>Row 3, Cell 3</td>
  </tr>
</table>
