#include <csv/csv_row_reader.h>

namespace columnar::csv {
namespace {
bool ToggleInQuotes(std::string_view line, bool in_quotes, char quote) {
    size_t pos = 0;
    while (pos < line.size()) {
        auto found = line.find(quote, pos);
        if (found == std::string_view::npos) {
            break;
        }
        if (in_quotes && found + 1 < line.size() && line[found + 1] == quote) {
            pos = found + 2;
            continue;
        }
        in_quotes = !in_quotes;
        pos = found + 1;
    }
    return in_quotes;
}

void ParseUnquotedRow(std::string_view raw_row, char delim, RowView& out) {
    size_t pos = 0;
    while (true) {
        auto next = raw_row.find(delim, pos);
        if (next == std::string_view::npos) {
            out.push_back({raw_row.substr(pos), false});
            return;
        }
        out.push_back({raw_row.substr(pos, next - pos), false});
        pos = next + 1;
        if (pos == raw_row.size()) {
            out.push_back({{}, false});
            return;
        }
    }
}

std::string_view ParseQuotedField(std::string_view raw_row, size_t& pos, char quote,
                                  std::string& scratch) {
    ++pos;
    auto closing = raw_row.find(quote, pos);
    if (closing == std::string_view::npos) {
        THROW_RUNTIME_ERROR("Got EOF inside quotes");
    }
    if (closing + 1 >= raw_row.size() || raw_row[closing + 1] != quote) {
        auto field = raw_row.substr(pos, closing - pos);
        pos = closing + 1;
        return field;
    }

    size_t start = scratch.size();
    while (pos < raw_row.size()) {
        auto next_quote = raw_row.find(quote, pos);
        if (next_quote == std::string_view::npos) {
            THROW_RUNTIME_ERROR("Got EOF inside the quotes");
        }
        scratch.append(raw_row.data() + pos, next_quote - pos);
        pos = next_quote + 1;
        if (pos < raw_row.size() && raw_row[pos] == quote) {
            scratch += quote;
            ++pos;
            continue;
        }
        break;
    }
    return std::string_view(scratch.data() + start, scratch.size() - start);
}

std::string_view ParseUnquotedField(std::string_view raw_row, size_t& pos, char delim) {
    auto next = raw_row.find(delim, pos);
    if (next == std::string_view::npos) {
        auto field = raw_row.substr(pos);
        pos = raw_row.size();
        return field;
    }
    auto field = raw_row.substr(pos, next - pos);
    pos = next;
    return field;
}

void ParseQuotedRow(std::string_view raw_row, const CSVOptions& options, std::string& scratch,
                    RowView& out) {
    scratch.reserve(raw_row.size());
    size_t pos = 0;
    while (pos < raw_row.size()) {
        bool was_quoted = raw_row[pos] == options.quote_char;
        auto field = was_quoted ? ParseQuotedField(raw_row, pos, options.quote_char, scratch)
                                : ParseUnquotedField(raw_row, pos, options.delimiter);
        out.push_back({field, was_quoted});

        if (pos < raw_row.size() && raw_row[pos] == options.delimiter) {
            ++pos;
            if (pos == raw_row.size()) {
                out.push_back({{}, false});
            }
        }
    }
}
}  // namespace

bool CSVRowReader::ReadRawRow() {
    if (!std::getline(is_, line_buf_)) {
        return false;
    }
    if (!line_buf_.empty() && line_buf_.back() == '\r') {
        line_buf_.pop_back();
    }

    raw_buf_ = line_buf_;
    bool in_quotes = ToggleInQuotes(raw_buf_, false, options_.quote_char);
    while (in_quotes && std::getline(is_, line_buf_)) {
        if (!line_buf_.empty() && line_buf_.back() == '\r') {
            line_buf_.pop_back();
        }
        raw_buf_ += '\n';
        raw_buf_ += line_buf_;
        in_quotes = ToggleInQuotes(line_buf_, in_quotes, options_.quote_char);
    }

    if (in_quotes) {
        THROW_RUNTIME_ERROR("Got EOF inside the quotes");
    }
    return true;
}

const RowView* CSVRowReader::ReadRowView() {
    if (!ReadRawRow()) {
        return nullptr;
    }

    parsed_fields_.clear();
    unescape_buf_.clear();

    std::string_view raw_row = raw_buf_;
    if (raw_row.find(options_.quote_char) == std::string_view::npos) {
        ParseUnquotedRow(raw_row, options_.delimiter, parsed_fields_);
    } else {
        ParseQuotedRow(raw_row, options_, unescape_buf_, parsed_fields_);
    }
    return &parsed_fields_;
}

std::optional<Row> CSVRowReader::ReadRow() {
    auto* view = ReadRowView();
    if (!view) {
        return std::nullopt;
    }
    Row row;
    row.reserve(view->size());
    for (const auto& field : *view) {
        row.push_back({std::string(field.value), field.was_quoted});
    }
    return row;
}
}  // namespace columnar::csv
