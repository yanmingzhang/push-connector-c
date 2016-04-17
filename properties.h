#ifndef PROPERTIES_H
#define PROPERTIES_H

#include <stdio.h>
#include <fstream>
#include <unordered_map>

class Properties
{
public:
    explicit Properties(const char *file) {
        std::ifstream stream(file);

        std::string line;
        while (std::getline(stream, line)) {
            if (line[0] == '\0' || line[0] == '#') {
                continue;
            }

            std::string::size_type index = line.find_first_of('=');
            if (index == std::string::npos) {
                continue;
            }

            std::string key = line.substr(0, index);
            std::string value = line.substr(index + 1);

            trim(key);
            trim(value);

            if (!key.empty()) {
                map_[key] = value;
            }
        }
    }

    ~Properties() = default;

    operator bool() const {
        return !map_.empty();
    }

    const char *get_property(const char *key) {
        auto itr = map_.find(key);
        if (itr == map_.end()) {
            return NULL;
        }

        return itr->second.c_str();
    }

private:
    void trim(std::string& s) {
        auto itr = s.begin();

        while (itr != s.end()) {
            if (!isspace(*itr)) {
                break;
            }
            ++itr;
        }

        s.erase(s.begin(), itr);

        auto ritr = s.rbegin();
        while (ritr != s.rend()) {
            if (!isspace(*ritr)) {
                break;
            }
            ritr++;
        }

        s.erase(ritr.base(), s.end());
    }

private:
    std::unordered_map<std::string, std::string> map_;
};

#endif // PROPERTIES_H
