package ui

import (
	"fmt"
	"strings"
)

// Alignment defines column text alignment.
type Alignment int

const (
	AlignLeft Alignment = iota
	AlignRight
	AlignCenter
)

// Table creates monospace brutalist ASCII tables.
type Table struct {
	Headers    []string
	Rows       [][]string
	Alignments []Alignment
}

// NewTable initializes a table with headers.
func NewTable(headers ...string) *Table {
	alignments := make([]Alignment, len(headers))
	for i := range alignments {
		alignments[i] = AlignLeft
	}
	return &Table{
		Headers:    headers,
		Rows:       make([][]string, 0),
		Alignments: alignments,
	}
}

// SetAlignment sets column text alignments.
func (t *Table) SetAlignment(alignments ...Alignment) *Table {
	t.Alignments = alignments
	return t
}

// AddRow adds a row of string cells.
func (t *Table) AddRow(cells ...string) *Table {
	t.Rows = append(t.Rows, cells)
	return t
}

// Render returns the brutalist ASCII table string.
func (t *Table) Render() string {
	colCount := len(t.Headers)
	if colCount == 0 {
		return ""
	}

	widths := make([]int, colCount)
	for i, h := range t.Headers {
		if len(h) > widths[i] {
			widths[i] = len(h)
		}
	}

	for _, row := range t.Rows {
		for i, cell := range row {
			if i < colCount && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	var sb strings.Builder

	// Separator line.
	renderSep := func(left, mid, right, fill string) {
		sb.WriteString(left)
		for i, w := range widths {
			sb.WriteString(strings.Repeat(fill, w+2))
			if i < colCount-1 {
				sb.WriteString(mid)
			}
		}
		sb.WriteString(right)
		sb.WriteString("\n")
	}

	// Format row.
	formatRow := func(cells []string) {
		sb.WriteString("|")
		for i, w := range widths {
			cell := ""
			if i < len(cells) {
				cell = cells[i]
			}
			align := AlignLeft
			if i < len(t.Alignments) {
				align = t.Alignments[i]
			}

			padLen := w - len(cell)
			if padLen < 0 {
				padLen = 0
			}

			sb.WriteString(" ")
			switch align {
			case AlignRight:
				sb.WriteString(strings.Repeat(" ", padLen))
				sb.WriteString(cell)
			case AlignCenter:
				leftPad := padLen / 2
				rightPad := padLen - leftPad
				sb.WriteString(strings.Repeat(" ", leftPad))
				sb.WriteString(cell)
				sb.WriteString(strings.Repeat(" ", rightPad))
			default: // AlignLeft
				sb.WriteString(cell)
				sb.WriteString(strings.Repeat(" ", padLen))
			}
			sb.WriteString(" |")
		}
		sb.WriteString("\n")
	}

	// Top border
	renderSep("+", "+", "+", "-")

	// Header row
	formatRow(t.Headers)

	// Header divider (double horizontal line style)
	renderSep("+", "+", "+", "=")

	// Rows
	for _, row := range t.Rows {
		formatRow(row)
	}

	// Bottom border
	renderSep("+", "+", "+", "-")

	return sb.String()
}

// Banner renders a brutalist header box.
func Banner(title string, metadata [][2]string) string {
	width := 78
	var sb strings.Builder

	sb.WriteString("+" + strings.Repeat("=", width-2) + "+\n")
	titleLine := fmt.Sprintf("|  %s", strings.ToUpper(title))
	if len(titleLine) < width-1 {
		titleLine += strings.Repeat(" ", width-1-len(titleLine)) + "|"
	} else {
		titleLine = titleLine[:width-1] + "|"
	}
	sb.WriteString(titleLine + "\n")
	sb.WriteString("+" + strings.Repeat("-", width-2) + "+\n")

	for _, meta := range metadata {
		k, v := meta[0], meta[1]
		line := fmt.Sprintf("|  %-24s : %s", k, v)
		if len(line) < width-1 {
			line += strings.Repeat(" ", width-1-len(line)) + "|"
		} else {
			line = line[:width-1] + "|"
		}
		sb.WriteString(line + "\n")
	}

	sb.WriteString("+" + strings.Repeat("=", width-2) + "+\n")
	return sb.String()
}

// StatusBadge formats a brutalist status string with brackets.
func StatusBadge(status string) string {
	switch status {
	case "HEALTHY", "STABLE":
		return "[HEALTHY]"
	case "FALLING_BEHIND":
		return "[FALLING_BEHIND]"
	case "STALLED":
		return "[STALLED]"
	case "PREPARING_REBALANCE", "PreparingRebalance":
		return "[PREPARING_REBALANCE]"
	case "COMPLETING_REBALANCE", "CompletingRebalance":
		return "[COMPLETING_REBALANCE]"
	case "DEAD", "Dead":
		return "[DEAD]"
	case "EMPTY", "Empty":
		return "[EMPTY]"
	default:
		return fmt.Sprintf("[%s]", strings.ToUpper(status))
	}
}
