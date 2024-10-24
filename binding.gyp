{
  "variables": {
      "os_linux_compiler%": "gcc",
      "use_vl32%": "false",
      "use_fixed_size%": "false",
      "use_posix_semaphores%": "false",
      "openssl_fips": ""
  },
  "targets": [
    {
      "target_name": "node-lmdb",
      "win_delay_load_hook": "false",
      "sources": [
        "libraries/liblmdb/mdb.c",
        "libraries/liblmdb/midl.c",
        "src/node-lmdb.cpp",
        "src/env.cpp",
        "src/misc.cpp",
        "src/txn.cpp",
        "src/dbi.cpp",
        "src/cursor.cpp"
      ],
      "include_dirs": [
        "<!(node -e \"require('nan')\")",
        "libraries/liblmdb"
      ],
      "conditions": [
        ["OS=='linux'", {
          "variables": {
            "gcc_version" : "<!(<(os_linux_compiler) -dumpversion | cut -d '.' -f 1)",
          },
          "conditions": [
            ["gcc_version>=7", {
              "cflags": [
                "-Wimplicit-fallthrough=2",
              ],
            }],
          ],
          "ldflags": [
            "-fPIC",
            "-fvisibility=hidden"
          ],
          "cflags": [
            "-fPIC",
            "-fvisibility=hidden"
          ],
          "cflags_cc": [
            "-fPIC",
            "-fvisibility=hidden",
            "-fvisibility-inlines-hidden",
          ]
        }],
        ["OS=='mac'", {
          "conditions": [
            ["use_posix_semaphores=='true'", {
              "defines": ["MDB_USE_POSIX_SEM"]
            }]
          ],
          "xcode_settings": {
            "MACOSX_DEPLOYMENT_TARGET": "10.7",
            "CLANG_CXX_LIBRARY": "libc++"
          }
        }],
        ["OS=='win'", {
            "libraries": ["ntdll.lib"]
        }],
        ["use_fixed_size=='true'", {
          "defines": ["MDB_FIXEDSIZE"],
        }],
        ["use_vl32=='true'", {
          "conditions": [
            ["target_arch=='ia32'", {
                "defines": ["MDB_VL32"]
              }]
            ]
        }],
      ],
    }
  ]
}
