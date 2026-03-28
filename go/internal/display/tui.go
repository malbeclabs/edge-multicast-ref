package display

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/malbeclabs/edge-multicast-ref/go/internal/stats"
)

// tickMsg triggers a periodic stats refresh in the TUI.
type tickMsg time.Time

// TUIModel is the bubbletea model for the stats dashboard.
type TUIModel struct {
	stats    *stats.Stats
	mu       *sync.RWMutex
	cancel   context.CancelFunc
	width    int
	height   int
	tickRate time.Duration

	// ExtraPanel is optional additional panel content (e.g. XDP stats).
	ExtraPanel string

	// Config values shown in the top bar.
	InterfaceName  string
	MulticastGroup string
}

// NewTUIModel creates a new TUI model.
func NewTUIModel(
	s *stats.Stats,
	mu *sync.RWMutex,
	cancel context.CancelFunc,
	refreshHz int,
	ifaceName, mcastGroup string,
) TUIModel {
	return TUIModel{
		stats:          s,
		mu:             mu,
		cancel:         cancel,
		tickRate:       time.Duration(1000/refreshHz) * time.Millisecond,
		InterfaceName:  ifaceName,
		MulticastGroup: mcastGroup,
	}
}

// Init returns the initial command (a periodic tick).
func (m TUIModel) Init() tea.Cmd {
	return tea.Tick(m.tickRate, func(t time.Time) tea.Msg { return tickMsg(t) })
}

// Update handles messages (key presses, window resize, tick).
func (m TUIModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			m.cancel()
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tickMsg:
		return m, tea.Tick(m.tickRate, func(t time.Time) tea.Msg { return tickMsg(t) })
	}
	return m, nil
}

// View renders the full TUI layout: top bar, slot table, bottom stats bar.
func (m TUIModel) View() string {
	if m.width == 0 {
		return "Initializing..."
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	innerWidth := m.width - 2 // account for border left+right

	borderStyle := lipgloss.NewStyle().
		Border(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		Width(innerWidth)

	titleStyle := lipgloss.NewStyle().Bold(true)

	// ── Top bar ──────────────────────────────────────────────────────
	uptime := FormatDurationShort(time.Since(m.stats.StartTime))
	hbInfo := fmt.Sprintf("heartbeats: %d", m.stats.TotalHeartbeats)
	if m.stats.LastHeartbeat != nil {
		hbInfo += fmt.Sprintf(" (last: %dms ago)", time.Since(*m.stats.LastHeartbeat).Milliseconds())
	} else {
		hbInfo += " (none yet)"
	}
	topContent := fmt.Sprintf(" iface: %s | group: %s | uptime: %s | %s",
		m.InterfaceName, m.MulticastGroup, uptime, hbInfo)

	topBar := borderStyle.Render(
		titleStyle.Render(" Edge Multicast Receiver ") + "\n" + topContent,
	)

	// ── Bottom stats bar ─────────────────────────────────────────────
	rate := m.stats.ShredsPerSecond()
	total := m.stats.TotalDataShreds + m.stats.TotalCodingShreds
	ratio := "n/a"
	if m.stats.TotalCodingShreds > 0 {
		ratio = fmt.Sprintf("%.1f", float64(m.stats.TotalDataShreds)/float64(m.stats.TotalCodingShreds))
	}
	bottomContent := fmt.Sprintf(
		" shreds/sec: %.0f | total: %d (data: %d, coding: %d) | data/coding: %s | errors: %d",
		rate, total, m.stats.TotalDataShreds, m.stats.TotalCodingShreds, ratio, m.stats.ParseErrors,
	)
	bottomBar := borderStyle.Render(
		titleStyle.Render(" Stats ") + "\n" + bottomContent,
	)

	// ── Slot table ───────────────────────────────────────────────────
	header := fmt.Sprintf(" %-12s %-14s %8s %8s %10s %8s",
		"Slot", "Signature", "Data", "Coding", "FEC Sets", "Age")

	// Compute how many data rows we can fit.
	topH := lipgloss.Height(topBar)
	bottomH := lipgloss.Height(bottomBar)
	extraH := 0
	if m.ExtraPanel != "" {
		extraH = lipgloss.Height(m.ExtraPanel) + 1 // +1 for newline separator
	}
	// 4 = slot-table border (top+bottom) + header line + newline between panels
	usedHeight := topH + bottomH + extraH + 4
	maxRows := m.height - usedHeight
	if maxRows < 1 {
		maxRows = 1
	}

	slots := m.stats.RecentSlots()
	var tableRows strings.Builder
	tableRows.WriteString(header)
	tableRows.WriteByte('\n')
	for i, ss := range slots {
		if i >= maxRows {
			break
		}
		age := FormatDurationShort(time.Since(ss.FirstSeen))
		fmt.Fprintf(&tableRows, " %-12d %-14s %8d %8d %10d %8s\n",
			ss.Slot, FormatSignaturePrefix(ss.SignaturePrefix),
			ss.DataShredCount, ss.CodingShredCount,
			ss.FECSetCount, age)
	}

	slotTable := borderStyle.Render(
		titleStyle.Render(" Recent Slots ") + "\n" + tableRows.String(),
	)

	// ── Compose layout ───────────────────────────────────────────────
	var out strings.Builder
	out.WriteString(topBar)
	out.WriteByte('\n')
	if m.ExtraPanel != "" {
		out.WriteString(m.ExtraPanel)
		out.WriteByte('\n')
	}
	out.WriteString(slotTable)
	out.WriteByte('\n')
	out.WriteString(bottomBar)
	return out.String()
}

// RunTUI starts the bubbletea TUI program. It blocks until the user quits or
// the context is cancelled.
func RunTUI(ctx context.Context, s *stats.Stats, mu *sync.RWMutex, refreshHz int, ifaceName, mcastGroup string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	model := NewTUIModel(s, mu, cancel, refreshHz, ifaceName, mcastGroup)
	p := tea.NewProgram(model, tea.WithAltScreen())

	done := make(chan error, 1)
	go func() {
		_, err := p.Run()
		done <- err
		cancel()
	}()

	select {
	case <-ctx.Done():
		p.Quit()
		return nil
	case err := <-done:
		return err
	}
}
