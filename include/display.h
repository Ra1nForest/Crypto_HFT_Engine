#pragma once

#include "orderbook.h"
#include "arbitrage_detector.h"
#include "latency_tracker.h"

#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>

class Display {
public:
    static void clearScreen() {
        std::cout << "\033[2J\033[1;1H";
    }

    static void render(
        const OrderBook& book_a,
        const OrderBook& book_b,
        const ArbitrageDetector& detector,
        const LatencyTracker& lat_a,
        const LatencyTracker& lat_b,
        bool live_a, bool live_b,
        int levels = 5
    ) {
        clearScreen();
        std::cout << std::fixed << std::setprecision(2);

        std::cout << "╔" << repeat("═", 87) << "╗" << std::endl;
        std::cout << "║" << center("Crypto HFT Engine v0.2", 87) << "║" << std::endl;
        std::cout << "╚" << repeat("═", 87) << "╝" << std::endl;
        std::cout << std::endl;

        printDualOrderBook(book_a, book_b, levels);
        std::cout << std::endl;

        printArbitragePanel(book_a, book_b, detector);
        std::cout << std::endl;

        std::cout << "═══ Performance ═══" << std::endl;
        std::cout << "  " << lat_a.getName() << ": " << lat_a.getCompactStats()
                  << "  " << (live_a ? "[\033[32mLIVE\033[0m]" : "[\033[31mDOWN\033[0m]")
                  << std::endl;
        std::cout << "  " << lat_b.getName() << ": " << lat_b.getCompactStats()
                  << "  " << (live_b ? "[\033[32mLIVE\033[0m]" : "[\033[31mDOWN\033[0m]")
                  << std::endl;
        std::cout << std::endl;
        std::cout << "Press Ctrl+C to exit" << std::endl;
    }

    static void printDualLatencyReport(const LatencyTracker& lat_a, const LatencyTracker& lat_b) {
        const int col_width = 42;
        std::string line = repeat("─", col_width);
        std::string gap = "   ";

        auto rowsA = buildLatencyRows(lat_a);
        auto rowsB = buildLatencyRows(lat_b);

        size_t max_rows = std::max(rowsA.size(), rowsB.size());
        while (rowsA.size() < max_rows) rowsA.push_back(padRow("", col_width));
        while (rowsB.size() < max_rows) rowsB.push_back(padRow("", col_width));

        std::cout << "  ┌" << line << "┐" << gap << "┌" << line << "┐" << std::endl;
        std::cout << "  │" << center(lat_a.getName() + " Latency", col_width) << "│"
                  << gap
                  << "│" << center(lat_b.getName() + " Latency", col_width) << "│" << std::endl;
        std::cout << "  ├" << line << "┤" << gap << "├" << line << "┤" << std::endl;

        for (size_t i = 0; i < max_rows; ++i) {
            std::cout << "  │" << rowsA[i] << "│" << gap << "│" << rowsB[i] << "│" << std::endl;
        }

        std::cout << "  └" << line << "┘" << gap << "└" << line << "┘" << std::endl;
    }

private:
    static std::string repeat(const std::string& s, int n) {
        std::string result;
        result.reserve(s.size() * n);
        for (int i = 0; i < n; ++i) result += s;
        return result;
    }

    static std::string fmtPrice(double price) {
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(2) << std::setw(10) << price;
        return ss.str();
    }

    static std::string center(const std::string& text, int width) {
        int pad = width - static_cast<int>(text.size());
        if (pad <= 0) return text.substr(0, width);
        int left = pad / 2;
        int right = pad - left;
        return std::string(left, ' ') + text + std::string(right, ' ');
    }

    static std::string padRow(const std::string& s, int width) {
        if (static_cast<int>(s.size()) >= width) return s.substr(0, width);
        return s + std::string(width - s.size(), ' ');
    }

    // ═══ Order Book 并排显示 ═══

    static void printDualOrderBook(const OrderBook& book_a, const OrderBook& book_b, int levels) {
        auto a_bids = collectBids(book_a, levels);
        auto a_asks = collectAsks(book_a, levels);
        auto b_bids = collectBids(book_b, levels);
        auto b_asks = collectAsks(book_b, levels);

        const int ob_width = 43;
        std::string line = repeat("─", ob_width);

        std::cout << "  ┌" << line << "┐ ┌" << line << "┐" << std::endl;
        std::cout << "  │" << center(book_a.getSymbol(), ob_width) << "│ │" << center(book_b.getSymbol(), ob_width) << "│" << std::endl;
        std::cout << "  │" << "    Qty       Price   │   Price        Qty " << "│ │" << "    Qty       Price   │   Price        Qty " << "│" << std::endl;
        std::cout << "  ├" << line << "┤ ├" << line << "┤" << std::endl;

        int max_rows = std::max({(int)a_bids.size(), (int)a_asks.size(),
                                  (int)b_bids.size(), (int)b_asks.size()});

        for (int i = 0; i < max_rows; ++i) {
            std::cout << "  │";
            std::cout << formatRow(a_bids, a_asks, i);
            std::cout << "│ │";
            std::cout << formatRow(b_bids, b_asks, i);
            std::cout << "│" << std::endl;
        }

        std::string spread_a = "Spread: " + fmtPrice(book_a.getSpread()) + "  Mid: " + fmtPrice((book_a.getBestBid().price + book_a.getBestAsk().price) / 2.0);
        std::string spread_b = "Spread: " + fmtPrice(book_b.getSpread()) + "  Mid: " + fmtPrice((book_b.getBestBid().price + book_b.getBestAsk().price) / 2.0);

        std::cout << "  ├" << line << "┤ ├" << line << "┤" << std::endl;
        std::cout << "  │" << center(spread_a, ob_width) << "│ │" << center(spread_b, ob_width) << "│" << std::endl;
        std::cout << "  └" << line << "┘ └" << line << "┘" << std::endl;
    }

    static std::string formatRow(const std::vector<PriceLevel>& bids,
                                  const std::vector<PriceLevel>& asks, int i) {
        std::ostringstream ss;
        ss << std::fixed;

        if (i < static_cast<int>(bids.size()) && bids[i].price > 0) {
            ss << std::setprecision(4) << std::setw(8) << bids[i].quantity
               << "  "
               << std::setprecision(2) << std::setw(10) << bids[i].price;
        } else {
            ss << std::setw(20) << " ";
        }

        ss << " │ ";

        if (i < static_cast<int>(asks.size()) && asks[i].price > 0) {
            ss << std::setprecision(2) << std::setw(10) << asks[i].price
               << "  "
               << std::setprecision(4) << std::setw(8) << asks[i].quantity;
        } else {
            ss << std::setw(20) << " ";
        }

        return ss.str();
    }

    static std::vector<PriceLevel> collectBids(const OrderBook& book, int levels) {
        std::vector<PriceLevel> result;
        auto& bids = book.getBids();
        int count = std::min(levels, static_cast<int>(bids.size()));
        for (int i = 0; i < count; ++i) {
            result.push_back(bids[i]);
        }
        return result;
    }

    static std::vector<PriceLevel> collectAsks(const OrderBook& book, int levels) {
        std::vector<PriceLevel> result;
        auto& asks = book.getAsks();
        int count = std::min(levels, static_cast<int>(asks.size()));
        for (int i = 0; i < count; ++i) {
            result.push_back(asks[i]);
        }
        return result;
    }

    // ═══ 套利面板 ═══

    static void printArbitragePanel(const OrderBook& book_a, const OrderBook& book_b,
                                     const ArbitrageDetector& detector) {
        auto a_ask = book_a.getBestAsk();
        auto a_bid = book_a.getBestBid();
        auto b_ask = book_b.getBestAsk();
        auto b_bid = book_b.getBestBid();

        double diff1 = b_bid.price - a_ask.price;
        double diff2 = a_bid.price - b_ask.price;

        std::string g = "\033[32m";
        std::string r = "\033[31m";
        std::string x = "\033[0m";
        std::string c1 = diff1 > 0 ? g : r;
        std::string c2 = diff2 > 0 ? g : r;

        std::string line = repeat("─", 89);

        std::ostringstream row1, row2, row3;
        row1 << std::fixed << std::setprecision(2)
             << "  Buy Binance  Ask: " << std::setw(10) << a_ask.price
             << "  ->  Sell OKX      Bid: " << std::setw(10) << b_bid.price
             << "  Diff: " << std::setw(10) << diff1 << "  ";

        row2 << std::fixed << std::setprecision(2)
             << "  Buy OKX      Ask: " << std::setw(10) << b_ask.price
             << "  ->  Sell Binance  Bid: " << std::setw(10) << a_bid.price
             << "  Diff: " << std::setw(10) << diff2 << "  ";

        row3 << "  Signals: " << std::setw(5) << detector.getTotalSignals()
             << "   |   Net Profit: $" << std::fixed << std::setprecision(2)
             << std::setw(10) << detector.getTotalNetProfit();

        std::string s1 = row1.str();
        std::string s2 = row2.str();
        std::string s3 = row3.str();
        int w = 89;
        if ((int)s1.size() < w) s1 += std::string(w - s1.size(), ' ');
        if ((int)s2.size() < w) s2 += std::string(w - s2.size(), ' ');
        if ((int)s3.size() < w) s3 += std::string(w - s3.size(), ' ');

        std::string d1 = s1, d2 = s2;
        size_t pos1 = d1.find("Diff:");
        size_t pos2 = d2.find("Diff:");
        if (pos1 != std::string::npos) { d1.insert(pos1, c1); d1 += x; }
        if (pos2 != std::string::npos) { d2.insert(pos2, c2); d2 += x; }

        std::cout << "  ┌" << line << "┐" << std::endl;
        std::cout << "  │" << center("Arbitrage Monitor", w) << "│" << std::endl;
        std::cout << "  ├" << line << "┤" << std::endl;
        std::cout << "  │" << d1 << "│" << std::endl;
        std::cout << "  │" << d2 << "│" << std::endl;
        std::cout << "  ├" << line << "┤" << std::endl;
        std::cout << "  │" << s3 << "│" << std::endl;
        std::cout << "  └" << line << "┘" << std::endl;
    }

    // ═══ 延迟报告 ═══

    static std::vector<std::string> buildLatencyRows(const LatencyTracker& lat) {
        const int w = 42;
        std::vector<std::string> rows;

        rows.push_back(makeStatRow("Messages:", std::to_string(lat.getCount()), "", w));
        rows.push_back(makeStatRow("Avg:", std::to_string(lat.getAvg()), "μs", w+1));
        rows.push_back(makeStatRow("Min:", std::to_string(lat.getMin()), "μs", w+1));
        rows.push_back(makeStatRow("P50:", std::to_string(lat.getP50()), "μs", w+1));
        rows.push_back(makeStatRow("P95:", std::to_string(lat.getP95()), "μs", w+1));
        rows.push_back(makeStatRow("P99:", std::to_string(lat.getP99()), "μs", w+1));
        rows.push_back(makeStatRow("Max:", std::to_string(lat.getMax()), "μs", w+1));

        rows.push_back(padRow(std::string(w, '-'), w));

        auto histRows = buildHistogramRows(lat);
        for (auto& hr : histRows) {
            rows.push_back(padRow(hr, w));
        }

        return rows;
    }

    static std::string makeStatRow(const std::string& label, const std::string& value,
                                     const std::string& unit, int width) {
        std::ostringstream ss;
        ss << "  " << std::setw(10) << std::left << label
           << std::setw(8) << std::right << value << " " << unit;
        std::string s = ss.str();
        if (static_cast<int>(s.size()) < width) s += std::string(width - s.size(), ' ');
        if (static_cast<int>(s.size()) > width) s = s.substr(0, width);
        return s;
    }

    static std::vector<std::string> buildHistogramRows(const LatencyTracker& lat) {
        std::vector<std::string> rows;

        auto samples = lat.getSamples();
        if (samples.empty()) {
            rows.push_back("  No data");
            return rows;
        }

        const std::vector<long> boundaries = {5, 10, 20, 50, 100};
        const std::vector<std::string> labels = {
            " 0-5  ", " 5-10 ", "10-20 ", "20-50 ", "50-100", " 100+ "
        };

        std::vector<int> buckets(boundaries.size() + 1, 0);
        for (long s : samples) {
            bool placed = false;
            for (size_t i = 0; i < boundaries.size(); ++i) {
                if (s < boundaries[i]) {
                    buckets[i]++;
                    placed = true;
                    break;
                }
            }
            if (!placed) buckets[boundaries.size()]++;
        }

        int max_count = *std::max_element(buckets.begin(), buckets.end());
        if (max_count == 0) max_count = 1;
        const int bar_max = 18;

        for (size_t i = 0; i < buckets.size(); ++i) {
            double pct = 100.0 * buckets[i] / samples.size();
            int bar_w = static_cast<int>(
                static_cast<double>(buckets[i]) / max_count * bar_max);

            std::ostringstream ss;
            ss << " " << labels[i];
            for (int j = 0; j < bar_w; ++j) ss << "#";
            for (int j = bar_w; j < bar_max; ++j) ss << " ";
            ss << std::setw(5) << std::fixed << std::setprecision(1) << pct << "%";

            rows.push_back(ss.str());
        }

        return rows;
    }
};