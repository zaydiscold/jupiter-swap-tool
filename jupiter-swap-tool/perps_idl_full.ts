export type Perpetuals = {
  "version": "0.1.0",
  "name": "perpetuals",
  "instructions": [
    {
      "name": "init",
      "accounts": [
        {
          "name": "upgradeAuthority",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "admin",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetualsProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetualsProgramData",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InitParams"
          }
        }
      ]
    },
    {
      "name": "addPool",
      "accounts": [
        {
          "name": "admin",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "rent",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "AddPoolParams"
          }
        }
      ]
    },
    {
      "name": "addCustody",
      "accounts": [
        {
          "name": "admin",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyTokenMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "rent",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "AddCustodyParams"
          }
        }
      ]
    },
    {
      "name": "setCustodyConfig",
      "accounts": [
        {
          "name": "admin",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "SetCustodyConfigParams"
          }
        }
      ]
    },
    {
      "name": "setPoolConfig",
      "accounts": [
        {
          "name": "admin",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "SetPoolConfigParams"
          }
        }
      ]
    },
    {
      "name": "setPerpetualsConfig",
      "accounts": [
        {
          "name": "admin",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "SetPerpetualsConfigParams"
          }
        }
      ]
    },
    {
      "name": "transferAdmin",
      "accounts": [
        {
          "name": "admin",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "newAdmin",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "TransferAdminParams"
          }
        }
      ]
    },
    {
      "name": "withdrawFees2",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "receivingTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "WithdrawFees2Params"
          }
        }
      ]
    },
    {
      "name": "createTokenMetadata",
      "accounts": [
        {
          "name": "admin",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "metadata",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenMetadataProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "rent",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "CreateTokenMetadataParams"
          }
        }
      ]
    },
    {
      "name": "createTokenLedger",
      "accounts": [
        {
          "name": "tokenLedger",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "payer",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": []
    },
    {
      "name": "reallocCustody",
      "accounts": [
        {
          "name": "keeper",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "rent",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": []
    },
    {
      "name": "reallocPool",
      "accounts": [
        {
          "name": "keeper",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "rent",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": []
    },
    {
      "name": "operatorSetCustodyConfig",
      "accounts": [
        {
          "name": "operator",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "OperatorSetCustodyConfigParams"
          }
        }
      ]
    },
    {
      "name": "operatorSetPoolConfig",
      "accounts": [
        {
          "name": "operator",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "OperatorSetPoolConfigParams"
          }
        }
      ]
    },
    {
      "name": "testInit",
      "accounts": [
        {
          "name": "upgradeAuthority",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "admin",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "TestInitParams"
          }
        }
      ]
    },
    {
      "name": "setTestTime",
      "accounts": [
        {
          "name": "admin",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "SetTestTimeParams"
          }
        }
      ]
    },
    {
      "name": "setTokenLedger",
      "accounts": [
        {
          "name": "tokenLedger",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": []
    },
    {
      "name": "swap2",
      "accounts": [
        {
          "name": "owner",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "fundingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "receivingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "receivingCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "receivingCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "receivingCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "receivingCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "dispensingCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "Swap2Params"
          }
        }
      ]
    },
    {
      "name": "addLiquidity2",
      "accounts": [
        {
          "name": "owner",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "fundingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "AddLiquidity2Params"
          }
        }
      ]
    },
    {
      "name": "removeLiquidity2",
      "accounts": [
        {
          "name": "owner",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "receivingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "RemoveLiquidity2Params"
          }
        }
      ]
    },
    {
      "name": "createIncreasePositionMarketRequest",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "fundingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "inputMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "CreateIncreasePositionMarketRequestParams"
          }
        }
      ]
    },
    {
      "name": "createDecreasePositionRequest2",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "receivingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "desiredMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "CreateDecreasePositionRequest2Params"
          }
        }
      ]
    },
    {
      "name": "createDecreasePositionMarketRequest",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "receivingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "desiredMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "CreateDecreasePositionMarketRequestParams"
          }
        }
      ]
    },
    {
      "name": "updateDecreasePositionRequest2",
      "accounts": [
        {
          "name": "owner",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "UpdateDecreasePositionRequest2Params"
          }
        }
      ]
    },
    {
      "name": "closePositionRequest",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true,
          "isOptional": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "ownerAta",
          "isMut": true,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "ClosePositionRequestParams"
          }
        }
      ]
    },
    {
      "name": "closePositionRequest2",
      "accounts": [
        {
          "name": "keeper",
          "isMut": true,
          "isSigner": true,
          "isOptional": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "ownerAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "mint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": []
    },
    {
      "name": "increasePosition4",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "IncreasePosition4Params"
          }
        }
      ]
    },
    {
      "name": "increasePositionPreSwap",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "keeperAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "instruction",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "IncreasePositionPreSwapParams"
          }
        }
      ]
    },
    {
      "name": "increasePositionWithInternalSwap",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "receivingCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "receivingCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "receivingCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "receivingCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "IncreasePositionWithInternalSwapParams"
          }
        }
      ]
    },
    {
      "name": "decreasePosition4",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "DecreasePosition4Params"
          }
        }
      ]
    },
    {
      "name": "decreasePositionWithInternalSwap",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "dispensingCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "DecreasePositionWithInternalSwapParams"
          }
        }
      ]
    },
    {
      "name": "decreasePositionWithTpsl",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "DecreasePositionWithTpslParams"
          }
        }
      ]
    },
    {
      "name": "decreasePositionWithTpslAndInternalSwap",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "dispensingCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "DecreasePositionWithTpslAndInternalSwapParams"
          }
        }
      ]
    },
    {
      "name": "liquidateFullPosition4",
      "accounts": [
        {
          "name": "signer",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "LiquidateFullPosition4Params"
          }
        }
      ]
    },
    {
      "name": "refreshAssetsUnderManagement",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "RefreshAssetsUnderManagementParams"
          }
        }
      ]
    },
    {
      "name": "setMaxGlobalSizes",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "SetMaxGlobalSizesParams"
          }
        }
      ]
    },
    {
      "name": "instantCreateTpsl",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "apiKeeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "receivingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "desiredMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InstantCreateTpslParams"
          }
        }
      ]
    },
    {
      "name": "instantCreateLimitOrder",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "apiKeeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "fundingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "inputMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InstantCreateLimitOrderParams"
          }
        }
      ]
    },
    {
      "name": "instantIncreasePosition",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "apiKeeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "fundingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenLedger",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InstantIncreasePositionParams"
          }
        }
      ]
    },
    {
      "name": "instantDecreasePosition",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "apiKeeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "receivingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "desiredMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InstantDecreasePositionParams"
          }
        }
      ]
    },
    {
      "name": "instantUpdateLimitOrder",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "apiKeeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InstantUpdateLimitOrderParams"
          }
        }
      ]
    },
    {
      "name": "instantUpdateTpsl",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "apiKeeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InstantUpdateTpslParams"
          }
        }
      ]
    },
    {
      "name": "getAddLiquidityAmountAndFee2",
      "accounts": [
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "GetAddLiquidityAmountAndFee2Params"
          }
        }
      ],
      "returns": {
        "defined": "AmountAndFee"
      }
    },
    {
      "name": "getRemoveLiquidityAmountAndFee2",
      "accounts": [
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "GetRemoveLiquidityAmountAndFee2Params"
          }
        }
      ],
      "returns": {
        "defined": "AmountAndFee"
      }
    },
    {
      "name": "getAssetsUnderManagement2",
      "accounts": [
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "GetAssetsUnderManagement2Params"
          }
        }
      ],
      "returns": "u128"
    },
    {
      "name": "borrowFromCustody",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "borrowPosition",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "userTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "BorrowFromCustodyParams"
          }
        }
      ]
    },
    {
      "name": "repayToCustody",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "borrowPosition",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "userTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "RepayToCustodyParams"
          }
        }
      ]
    },
    {
      "name": "depositCollateralForBorrows",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "borrowPosition",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "userTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "DepositParams"
          }
        }
      ]
    },
    {
      "name": "withdrawCollateralForBorrows",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "borrowPosition",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "userTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "WithdrawParams"
          }
        }
      ]
    },
    {
      "name": "liquidateBorrowPosition",
      "accounts": [
        {
          "name": "signer",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "borrowPosition",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "LiquidateBorrowPositionParams"
          }
        }
      ]
    },
    {
      "name": "closeBorrowPosition",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "borrowPosition",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "CloseBorrowPositionParams"
          }
        }
      ]
    }
  ],
  "accounts": [
    {
      "name": "borrowPosition",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "owner",
            "type": "publicKey"
          },
          {
            "name": "pool",
            "type": "publicKey"
          },
          {
            "name": "custody",
            "type": "publicKey"
          },
          {
            "name": "openTime",
            "type": "i64"
          },
          {
            "name": "updateTime",
            "type": "i64"
          },
          {
            "name": "borrowSize",
            "type": "u128"
          },
          {
            "name": "cumulativeCompoundedInterestSnapshot",
            "type": "u128"
          },
          {
            "name": "lockedCollateral",
            "type": "u64"
          },
          {
            "name": "bump",
            "type": "u8"
          },
          {
            "name": "lastBorrowed",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "custody",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "pool",
            "type": "publicKey"
          },
          {
            "name": "mint",
            "type": "publicKey"
          },
          {
            "name": "tokenAccount",
            "type": "publicKey"
          },
          {
            "name": "decimals",
            "type": "u8"
          },
          {
            "name": "isStable",
            "type": "bool"
          },
          {
            "name": "oracle",
            "type": {
              "defined": "OracleParams"
            }
          },
          {
            "name": "pricing",
            "type": {
              "defined": "PricingParams"
            }
          },
          {
            "name": "permissions",
            "type": {
              "defined": "Permissions"
            }
          },
          {
            "name": "targetRatioBps",
            "type": "u64"
          },
          {
            "name": "assets",
            "type": {
              "defined": "Assets"
            }
          },
          {
            "name": "fundingRateState",
            "type": {
              "defined": "FundingRateState"
            }
          },
          {
            "name": "bump",
            "type": "u8"
          },
          {
            "name": "tokenAccountBump",
            "type": "u8"
          },
          {
            "name": "increasePositionBps",
            "type": "u64"
          },
          {
            "name": "decreasePositionBps",
            "type": "u64"
          },
          {
            "name": "maxPositionSizeUsd",
            "type": "u64"
          },
          {
            "name": "dovesOracle",
            "type": "publicKey"
          },
          {
            "name": "jumpRateState",
            "type": {
              "defined": "JumpRateState"
            }
          },
          {
            "name": "dovesAgOracle",
            "type": "publicKey"
          },
          {
            "name": "priceImpactBuffer",
            "type": {
              "defined": "PriceImpactBuffer"
            }
          },
          {
            "name": "borrowLendParameters",
            "type": {
              "defined": "BorrowLendParams"
            }
          },
          {
            "name": "borrowsFundingRateState",
            "type": {
              "defined": "FundingRateState"
            }
          },
          {
            "name": "debt",
            "type": "u128"
          },
          {
            "name": "borrowLendInterestsAccured",
            "type": "u128"
          },
          {
            "name": "borrowLimitInTokenAmount",
            "type": "u64"
          },
          {
            "name": "minInterestFeeBps",
            "type": "u64"
          },
          {
            "name": "minInterestFeeGracePeriodSeconds",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "perpetuals",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "permissions",
            "type": {
              "defined": "Permissions"
            }
          },
          {
            "name": "pools",
            "type": {
              "vec": "publicKey"
            }
          },
          {
            "name": "admin",
            "type": "publicKey"
          },
          {
            "name": "transferAuthorityBump",
            "type": "u8"
          },
          {
            "name": "perpetualsBump",
            "type": "u8"
          },
          {
            "name": "inceptionTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "pool",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "name",
            "type": "string"
          },
          {
            "name": "custodies",
            "type": {
              "vec": "publicKey"
            }
          },
          {
            "name": "aumUsd",
            "type": "u128"
          },
          {
            "name": "limit",
            "type": {
              "defined": "Limit"
            }
          },
          {
            "name": "fees",
            "type": {
              "defined": "Fees"
            }
          },
          {
            "name": "poolApr",
            "type": {
              "defined": "PoolApr"
            }
          },
          {
            "name": "maxRequestExecutionSec",
            "type": "i64"
          },
          {
            "name": "bump",
            "type": "u8"
          },
          {
            "name": "lpTokenBump",
            "type": "u8"
          },
          {
            "name": "inceptionTime",
            "type": "i64"
          },
          {
            "name": "parameterUpdateOracle",
            "type": {
              "defined": "Secp256k1Pubkey"
            }
          },
          {
            "name": "aumUsdUpdatedAt",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "positionRequest",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "owner",
            "type": "publicKey"
          },
          {
            "name": "pool",
            "type": "publicKey"
          },
          {
            "name": "custody",
            "type": "publicKey"
          },
          {
            "name": "position",
            "type": "publicKey"
          },
          {
            "name": "mint",
            "type": "publicKey"
          },
          {
            "name": "openTime",
            "type": "i64"
          },
          {
            "name": "updateTime",
            "type": "i64"
          },
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "collateralDelta",
            "type": "u64"
          },
          {
            "name": "requestChange",
            "type": {
              "defined": "RequestChange"
            }
          },
          {
            "name": "requestType",
            "type": {
              "defined": "RequestType"
            }
          },
          {
            "name": "side",
            "type": {
              "defined": "Side"
            }
          },
          {
            "name": "priceSlippage",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "jupiterMinimumOut",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "preSwapAmount",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "triggerPrice",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "triggerAboveThreshold",
            "type": {
              "option": "bool"
            }
          },
          {
            "name": "entirePosition",
            "type": {
              "option": "bool"
            }
          },
          {
            "name": "executed",
            "type": "bool"
          },
          {
            "name": "counter",
            "type": "u64"
          },
          {
            "name": "bump",
            "type": "u8"
          },
          {
            "name": "referral",
            "type": {
              "option": "publicKey"
            }
          }
        ]
      }
    },
    {
      "name": "position",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "owner",
            "type": "publicKey"
          },
          {
            "name": "pool",
            "type": "publicKey"
          },
          {
            "name": "custody",
            "type": "publicKey"
          },
          {
            "name": "collateralCustody",
            "type": "publicKey"
          },
          {
            "name": "openTime",
            "type": "i64"
          },
          {
            "name": "updateTime",
            "type": "i64"
          },
          {
            "name": "side",
            "type": {
              "defined": "Side"
            }
          },
          {
            "name": "price",
            "type": "u64"
          },
          {
            "name": "sizeUsd",
            "type": "u64"
          },
          {
            "name": "collateralUsd",
            "type": "u64"
          },
          {
            "name": "realisedPnlUsd",
            "type": "i64"
          },
          {
            "name": "cumulativeInterestSnapshot",
            "type": "u128"
          },
          {
            "name": "lockedAmount",
            "type": "u64"
          },
          {
            "name": "bump",
            "type": "u8"
          }
        ]
      }
    },
    {
      "name": "tokenLedger",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "tokenAccount",
            "type": "publicKey"
          },
          {
            "name": "amount",
            "type": "u64"
          }
        ]
      }
    }
  ],
  "types": [
    {
      "name": "AddCustodyParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "isStable",
            "type": "bool"
          },
          {
            "name": "oracle",
            "type": {
              "defined": "OracleParams"
            }
          },
          {
            "name": "pricing",
            "type": {
              "defined": "PricingParams"
            }
          },
          {
            "name": "permissions",
            "type": {
              "defined": "Permissions"
            }
          },
          {
            "name": "hourlyFundingDbps",
            "type": "u64"
          },
          {
            "name": "targetRatioBps",
            "type": "u64"
          },
          {
            "name": "increasePositionBps",
            "type": "u64"
          },
          {
            "name": "decreasePositionBps",
            "type": "u64"
          },
          {
            "name": "dovesOracle",
            "type": "publicKey"
          },
          {
            "name": "maxPositionSizeUsd",
            "type": "u64"
          },
          {
            "name": "jumpRate",
            "type": {
              "defined": "JumpRateState"
            }
          },
          {
            "name": "priceImpactFeeFactor",
            "type": "u64"
          },
          {
            "name": "priceImpactExponent",
            "type": "f32"
          },
          {
            "name": "deltaImbalanceThresholdDecimal",
            "type": "u64"
          },
          {
            "name": "maxFeeBps",
            "type": "u64"
          },
          {
            "name": "dovesAgOracle",
            "type": "publicKey"
          }
        ]
      }
    },
    {
      "name": "AddLiquidity2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "tokenAmountIn",
            "type": "u64"
          },
          {
            "name": "minLpAmountOut",
            "type": "u64"
          },
          {
            "name": "tokenAmountPreSwap",
            "type": {
              "option": "u64"
            }
          }
        ]
      }
    },
    {
      "name": "AddPoolParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "name",
            "type": "string"
          },
          {
            "name": "limit",
            "type": {
              "defined": "Limit"
            }
          },
          {
            "name": "fees",
            "type": {
              "defined": "Fees"
            }
          },
          {
            "name": "maxRequestExecutionSec",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "BorrowFromCustodyParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "amount",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "CloseBorrowPositionParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "ClosePositionRequestParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "CreateDecreasePositionMarketRequestParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "collateralUsdDelta",
            "type": "u64"
          },
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "priceSlippage",
            "type": "u64"
          },
          {
            "name": "jupiterMinimumOut",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "entirePosition",
            "type": {
              "option": "bool"
            }
          },
          {
            "name": "counter",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "CreateDecreasePositionRequest2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "collateralUsdDelta",
            "type": "u64"
          },
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "requestType",
            "type": {
              "defined": "RequestType"
            }
          },
          {
            "name": "priceSlippage",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "jupiterMinimumOut",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "triggerPrice",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "triggerAboveThreshold",
            "type": {
              "option": "bool"
            }
          },
          {
            "name": "entirePosition",
            "type": {
              "option": "bool"
            }
          },
          {
            "name": "counter",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "CreateIncreasePositionMarketRequestParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "collateralTokenDelta",
            "type": "u64"
          },
          {
            "name": "side",
            "type": {
              "defined": "Side"
            }
          },
          {
            "name": "priceSlippage",
            "type": "u64"
          },
          {
            "name": "jupiterMinimumOut",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "counter",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "CreateTokenMetadataParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "name",
            "type": "string"
          },
          {
            "name": "symbol",
            "type": "string"
          },
          {
            "name": "uri",
            "type": "string"
          }
        ]
      }
    },
    {
      "name": "DecreasePosition4Params",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "DecreasePositionWithInternalSwapParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "DecreasePositionWithTpslAndInternalSwapParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "DecreasePositionWithTpslParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "DepositParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "amount",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "GetAddLiquidityAmountAndFee2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "tokenAmountIn",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "GetAssetsUnderManagement2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "mode",
            "type": {
              "option": {
                "defined": "PriceCalcMode"
              }
            }
          }
        ]
      }
    },
    {
      "name": "GetRemoveLiquidityAmountAndFee2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "lpAmountIn",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "IncreasePosition4Params",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "IncreasePositionPreSwapParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "IncreasePositionWithInternalSwapParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "InitParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "allowSwap",
            "type": "bool"
          },
          {
            "name": "allowAddLiquidity",
            "type": "bool"
          },
          {
            "name": "allowRemoveLiquidity",
            "type": "bool"
          },
          {
            "name": "allowIncreasePosition",
            "type": "bool"
          },
          {
            "name": "allowDecreasePosition",
            "type": "bool"
          },
          {
            "name": "allowCollateralWithdrawal",
            "type": "bool"
          },
          {
            "name": "allowLiquidatePosition",
            "type": "bool"
          }
        ]
      }
    },
    {
      "name": "InstantCreateLimitOrderParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "collateralTokenDelta",
            "type": "u64"
          },
          {
            "name": "side",
            "type": {
              "defined": "Side"
            }
          },
          {
            "name": "triggerPrice",
            "type": "u64"
          },
          {
            "name": "triggerAboveThreshold",
            "type": "bool"
          },
          {
            "name": "counter",
            "type": "u64"
          },
          {
            "name": "requestTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "InstantCreateTpslParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "collateralUsdDelta",
            "type": "u64"
          },
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "triggerPrice",
            "type": "u64"
          },
          {
            "name": "triggerAboveThreshold",
            "type": "bool"
          },
          {
            "name": "entirePosition",
            "type": "bool"
          },
          {
            "name": "counter",
            "type": "u64"
          },
          {
            "name": "requestTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "InstantDecreasePositionParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "collateralUsdDelta",
            "type": "u64"
          },
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "priceSlippage",
            "type": "u64"
          },
          {
            "name": "entirePosition",
            "type": {
              "option": "bool"
            }
          },
          {
            "name": "requestTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "InstantIncreasePositionParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "collateralTokenDelta",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "side",
            "type": {
              "defined": "Side"
            }
          },
          {
            "name": "priceSlippage",
            "type": "u64"
          },
          {
            "name": "requestTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "InstantUpdateLimitOrderParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "triggerPrice",
            "type": "u64"
          },
          {
            "name": "requestTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "InstantUpdateTpslParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "triggerPrice",
            "type": "u64"
          },
          {
            "name": "requestTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "LiquidateBorrowPositionParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "LiquidateFullPosition4Params",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "OperatorSetCustodyConfigParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "pricing",
            "type": {
              "defined": "PricingParams"
            }
          },
          {
            "name": "hourlyFundingDbps",
            "type": "u64"
          },
          {
            "name": "targetRatioBps",
            "type": "u64"
          },
          {
            "name": "increasePositionBps",
            "type": "u64"
          },
          {
            "name": "decreasePositionBps",
            "type": "u64"
          },
          {
            "name": "maxPositionSizeUsd",
            "type": "u64"
          },
          {
            "name": "jumpRate",
            "type": {
              "defined": "JumpRateState"
            }
          },
          {
            "name": "priceImpactFeeFactor",
            "type": "u64"
          },
          {
            "name": "priceImpactExponent",
            "type": "f32"
          },
          {
            "name": "deltaImbalanceThresholdDecimal",
            "type": "u64"
          },
          {
            "name": "maxFeeBps",
            "type": "u64"
          },
          {
            "name": "borrowLendParameters",
            "type": {
              "defined": "BorrowLendParams"
            }
          },
          {
            "name": "borrowHourlyFundingDbps",
            "type": "u64"
          },
          {
            "name": "borrowLimitInTokenAmount",
            "type": "u64"
          },
          {
            "name": "minInterestFeeBps",
            "type": "u64"
          },
          {
            "name": "minInterestFeeGracePeriodSeconds",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "OperatorSetPoolConfigParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "fees",
            "type": {
              "defined": "Fees"
            }
          },
          {
            "name": "limit",
            "type": {
              "defined": "Limit"
            }
          },
          {
            "name": "maxRequestExecutionSec",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "RefreshAssetsUnderManagementParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "RemoveLiquidity2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "lpAmountIn",
            "type": "u64"
          },
          {
            "name": "minAmountOut",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "RepayToCustodyParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "amount",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "SetCustodyConfigParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "oracle",
            "type": {
              "defined": "OracleParams"
            }
          },
          {
            "name": "pricing",
            "type": {
              "defined": "PricingParams"
            }
          },
          {
            "name": "permissions",
            "type": {
              "defined": "Permissions"
            }
          },
          {
            "name": "hourlyFundingDbps",
            "type": "u64"
          },
          {
            "name": "targetRatioBps",
            "type": "u64"
          },
          {
            "name": "increasePositionBps",
            "type": "u64"
          },
          {
            "name": "decreasePositionBps",
            "type": "u64"
          },
          {
            "name": "dovesOracle",
            "type": "publicKey"
          },
          {
            "name": "maxPositionSizeUsd",
            "type": "u64"
          },
          {
            "name": "jumpRate",
            "type": {
              "defined": "JumpRateState"
            }
          },
          {
            "name": "priceImpactFeeFactor",
            "type": "u64"
          },
          {
            "name": "priceImpactExponent",
            "type": "f32"
          },
          {
            "name": "deltaImbalanceThresholdDecimal",
            "type": "u64"
          },
          {
            "name": "maxFeeBps",
            "type": "u64"
          },
          {
            "name": "dovesAgOracle",
            "type": "publicKey"
          },
          {
            "name": "borrowLendParameters",
            "type": {
              "defined": "BorrowLendParams"
            }
          },
          {
            "name": "borrowHourlyFundingDbps",
            "type": "u64"
          },
          {
            "name": "borrowLimitInTokenAmount",
            "type": "u64"
          },
          {
            "name": "minInterestFeeBps",
            "type": "u64"
          },
          {
            "name": "minInterestFeeGracePeriodSeconds",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "SetMaxGlobalSizesParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "maxGlobalLongSize",
            "type": "u64"
          },
          {
            "name": "maxGlobalShortSize",
            "type": "u64"
          },
          {
            "name": "recoveryId",
            "type": "u8"
          },
          {
            "name": "signature",
            "type": {
              "array": [
                "u8",
                64
              ]
            }
          },
          {
            "name": "referenceId",
            "type": {
              "array": [
                "u8",
                16
              ]
            }
          },
          {
            "name": "timestamp",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "SetPerpetualsConfigParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "permissions",
            "type": {
              "defined": "Permissions"
            }
          }
        ]
      }
    },
    {
      "name": "SetPoolConfigParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "fees",
            "type": {
              "defined": "Fees"
            }
          },
          {
            "name": "limit",
            "type": {
              "defined": "Limit"
            }
          },
          {
            "name": "maxRequestExecutionSec",
            "type": "i64"
          },
          {
            "name": "parameterUpdateOracle",
            "type": {
              "defined": "Secp256k1Pubkey"
            }
          }
        ]
      }
    },
    {
      "name": "SetTestTimeParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "time",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "Swap2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "amountIn",
            "type": "u64"
          },
          {
            "name": "minAmountOut",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "TestInitParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "allowSwap",
            "type": "bool"
          },
          {
            "name": "allowAddLiquidity",
            "type": "bool"
          },
          {
            "name": "allowRemoveLiquidity",
            "type": "bool"
          },
          {
            "name": "allowIncreasePosition",
            "type": "bool"
          },
          {
            "name": "allowDecreasePosition",
            "type": "bool"
          },
          {
            "name": "allowCollateralWithdrawal",
            "type": "bool"
          },
          {
            "name": "allowLiquidatePosition",
            "type": "bool"
          }
        ]
      }
    },
    {
      "name": "TransferAdminParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "UpdateDecreasePositionRequest2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "triggerPrice",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "WithdrawParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "amount",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "WithdrawFees2Params",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "PriceImpactBuffer",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "openInterest",
            "type": {
              "array": [
                "i64",
                60
              ]
            }
          },
          {
            "name": "lastUpdated",
            "type": "i64"
          },
          {
            "name": "feeFactor",
            "type": "u64"
          },
          {
            "name": "exponent",
            "type": "f32"
          },
          {
            "name": "deltaImbalanceThresholdDecimal",
            "type": "u64"
          },
          {
            "name": "maxFeeBps",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "Assets",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "feesReserves",
            "type": "u64"
          },
          {
            "name": "owned",
            "type": "u64"
          },
          {
            "name": "locked",
            "type": "u64"
          },
          {
            "name": "guaranteedUsd",
            "type": "u64"
          },
          {
            "name": "globalShortSizes",
            "type": "u64"
          },
          {
            "name": "globalShortAveragePrices",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "PricingParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "tradeImpactFeeScalar",
            "type": "u64"
          },
          {
            "name": "buffer",
            "type": "u64"
          },
          {
            "name": "swapSpread",
            "type": "u64"
          },
          {
            "name": "maxLeverage",
            "type": "u64"
          },
          {
            "name": "maxGlobalLongSizes",
            "type": "u64"
          },
          {
            "name": "maxGlobalShortSizes",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "FundingRateState",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "cumulativeInterestRate",
            "type": "u128"
          },
          {
            "name": "lastUpdate",
            "type": "i64"
          },
          {
            "name": "hourlyFundingDbps",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "JumpRateState",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "minRateBps",
            "type": "u64"
          },
          {
            "name": "maxRateBps",
            "type": "u64"
          },
          {
            "name": "targetRateBps",
            "type": "u64"
          },
          {
            "name": "targetUtilizationRate",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "BorrowLendParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "borrowsLimitInBps",
            "type": "u64"
          },
          {
            "name": "maintainanceMarginBps",
            "type": "u64"
          },
          {
            "name": "protocolFeeBps",
            "type": "u64"
          },
          {
            "name": "liquidationMargin",
            "type": "u64"
          },
          {
            "name": "liquidationFeeBps",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "OraclePrice",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "price",
            "type": "u64"
          },
          {
            "name": "exponent",
            "type": "i32"
          }
        ]
      }
    },
    {
      "name": "Price",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "price",
            "type": "u64"
          },
          {
            "name": "expo",
            "type": "i32"
          },
          {
            "name": "publishTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "OracleParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "oracleAccount",
            "type": "publicKey"
          },
          {
            "name": "oracleType",
            "type": {
              "defined": "OracleType"
            }
          },
          {
            "name": "buffer",
            "type": "u64"
          },
          {
            "name": "maxPriceAgeSec",
            "type": "u32"
          }
        ]
      }
    },
    {
      "name": "AmountAndFee",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "amount",
            "type": "u64"
          },
          {
            "name": "fee",
            "type": "u64"
          },
          {
            "name": "feeBps",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "Permissions",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "allowSwap",
            "type": "bool"
          },
          {
            "name": "allowAddLiquidity",
            "type": "bool"
          },
          {
            "name": "allowRemoveLiquidity",
            "type": "bool"
          },
          {
            "name": "allowIncreasePosition",
            "type": "bool"
          },
          {
            "name": "allowDecreasePosition",
            "type": "bool"
          },
          {
            "name": "allowCollateralWithdrawal",
            "type": "bool"
          },
          {
            "name": "allowLiquidatePosition",
            "type": "bool"
          }
        ]
      }
    },
    {
      "name": "Fees",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "swapMultiplier",
            "type": "u64"
          },
          {
            "name": "stableSwapMultiplier",
            "type": "u64"
          },
          {
            "name": "addRemoveLiquidityBps",
            "type": "u64"
          },
          {
            "name": "swapBps",
            "type": "u64"
          },
          {
            "name": "taxBps",
            "type": "u64"
          },
          {
            "name": "stableSwapBps",
            "type": "u64"
          },
          {
            "name": "stableSwapTaxBps",
            "type": "u64"
          },
          {
            "name": "liquidationRewardBps",
            "type": "u64"
          },
          {
            "name": "protocolShareBps",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "PoolApr",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "lastUpdated",
            "type": "i64"
          },
          {
            "name": "feeAprBps",
            "type": "u64"
          },
          {
            "name": "realizedFeeUsd",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "Limit",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "maxAumUsd",
            "type": "u128"
          },
          {
            "name": "tokenWeightageBufferBps",
            "type": "u128"
          },
          {
            "name": "buffer",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "Secp256k1Pubkey",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "prefix",
            "type": "u8"
          },
          {
            "name": "key",
            "type": {
              "array": [
                "u8",
                32
              ]
            }
          }
        ]
      }
    },
    {
      "name": "PriceImpactMechanism",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "TradeSize"
          },
          {
            "name": "DeltaImbalance"
          }
        ]
      }
    },
    {
      "name": "OracleType",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "None"
          },
          {
            "name": "Test"
          },
          {
            "name": "Pyth"
          }
        ]
      }
    },
    {
      "name": "PriceCalcMode",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "Min"
          },
          {
            "name": "Max"
          },
          {
            "name": "Ignore"
          }
        ]
      }
    },
    {
      "name": "PriceStaleTolerance",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "Strict"
          },
          {
            "name": "Loose"
          }
        ]
      }
    },
    {
      "name": "TradePoolType",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "Increase"
          },
          {
            "name": "Decrease"
          }
        ]
      }
    },
    {
      "name": "RequestType",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "Market"
          },
          {
            "name": "Trigger"
          }
        ]
      }
    },
    {
      "name": "RequestChange",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "None"
          },
          {
            "name": "Increase"
          },
          {
            "name": "Decrease"
          }
        ]
      }
    },
    {
      "name": "Side",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "None"
          },
          {
            "name": "Long"
          },
          {
            "name": "Short"
          }
        ]
      }
    }
  ],
  "events": [
    {
      "name": "CreatePositionRequestEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceSlippage",
          "type": {
            "option": "u64"
          },
          "index": false
        },
        {
          "name": "jupiterMinimumOut",
          "type": {
            "option": "u64"
          },
          "index": false
        },
        {
          "name": "preSwapAmount",
          "type": {
            "option": "u64"
          },
          "index": false
        },
        {
          "name": "requestChange",
          "type": "u8",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "referral",
          "type": {
            "option": "publicKey"
          },
          "index": false
        }
      ]
    },
    {
      "name": "InstantCreateTpslEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "entirePosition",
          "type": "bool",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "InstantUpdateTpslEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "entirePosition",
          "type": "bool",
          "index": false
        },
        {
          "name": "updateTime",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "ClosePositionRequestEvent",
      "fields": [
        {
          "name": "entirePosition",
          "type": {
            "option": "bool"
          },
          "index": false
        },
        {
          "name": "executed",
          "type": "bool",
          "index": false
        },
        {
          "name": "requestChange",
          "type": "u8",
          "index": false
        },
        {
          "name": "requestType",
          "type": "u8",
          "index": false
        },
        {
          "name": "side",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "mint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "amount",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "IncreasePositionEvent",
      "fields": [
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSizeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestChange",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionRequestType",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionRequestCollateralDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralTokenDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "price",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceSlippage",
          "type": {
            "option": "u64"
          },
          "index": false
        },
        {
          "name": "feeToken",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "referral",
          "type": {
            "option": "publicKey"
          },
          "index": false
        },
        {
          "name": "positionFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "fundingFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceImpactFeeUsd",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "IncreasePositionPreSwapEvent",
      "fields": [
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "transferAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralCustodyPreSwapAmount",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "DecreasePositionEvent",
      "fields": [
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSizeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestChange",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionRequestType",
          "type": "u8",
          "index": false
        },
        {
          "name": "hasProfit",
          "type": "bool",
          "index": false
        },
        {
          "name": "pnlDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "transferAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "transferToken",
          "type": {
            "option": "u64"
          },
          "index": false
        },
        {
          "name": "price",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceSlippage",
          "type": {
            "option": "u64"
          },
          "index": false
        },
        {
          "name": "feeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "referral",
          "type": {
            "option": "publicKey"
          },
          "index": false
        },
        {
          "name": "positionFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "fundingFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceImpactFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "originalPositionCollateralUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionCollateralUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionOpenTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "positionPrice",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "DecreasePositionPostSwapEvent",
      "fields": [
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "swapAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "jupiterMinimumOut",
          "type": {
            "option": "u64"
          },
          "index": false
        }
      ]
    },
    {
      "name": "LiquidateFullPositionEvent",
      "fields": [
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSizeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "hasProfit",
          "type": "bool",
          "index": false
        },
        {
          "name": "pnlDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "transferAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "transferToken",
          "type": "u64",
          "index": false
        },
        {
          "name": "price",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "liquidationFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "positionFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "fundingFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceImpactFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "originalPositionCollateralUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionCollateralUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionOpenTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "positionPrice",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "PoolSwapEvent",
      "fields": [
        {
          "name": "receivingCustodyKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "dispensingCustodyKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "poolKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "amountIn",
          "type": "u64",
          "index": false
        },
        {
          "name": "amountOut",
          "type": "u64",
          "index": false
        },
        {
          "name": "swapUsdAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "amountOutAfterFees",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeBps",
          "type": "u64",
          "index": false
        },
        {
          "name": "ownerKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "receivingAccountKey",
          "type": "publicKey",
          "index": false
        }
      ]
    },
    {
      "name": "PoolSwapExactOutEvent",
      "fields": [
        {
          "name": "receivingCustodyKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "dispensingCustodyKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "poolKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "amountIn",
          "type": "u64",
          "index": false
        },
        {
          "name": "amountInAfterFees",
          "type": "u64",
          "index": false
        },
        {
          "name": "amountOut",
          "type": "u64",
          "index": false
        },
        {
          "name": "swapUsdAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeBps",
          "type": "u64",
          "index": false
        },
        {
          "name": "ownerKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "receivingAccountKey",
          "type": "publicKey",
          "index": false
        }
      ]
    },
    {
      "name": "AddLiquidityEvent",
      "fields": [
        {
          "name": "custodyKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "poolKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "tokenAmountIn",
          "type": "u64",
          "index": false
        },
        {
          "name": "prePoolAmountUsd",
          "type": "u128",
          "index": false
        },
        {
          "name": "tokenAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeBps",
          "type": "u64",
          "index": false
        },
        {
          "name": "tokenAmountAfterFee",
          "type": "u64",
          "index": false
        },
        {
          "name": "mintAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "lpAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "postPoolAmountUsd",
          "type": "u128",
          "index": false
        }
      ]
    },
    {
      "name": "RemoveLiquidityEvent",
      "fields": [
        {
          "name": "custodyKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "poolKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "lpAmountIn",
          "type": "u64",
          "index": false
        },
        {
          "name": "removeAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeBps",
          "type": "u64",
          "index": false
        },
        {
          "name": "removeTokenAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "tokenAmountAfterFee",
          "type": "u64",
          "index": false
        },
        {
          "name": "postPoolAmountUsd",
          "type": "u128",
          "index": false
        }
      ]
    },
    {
      "name": "InstantCreateLimitOrderEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "InstantIncreasePositionEvent",
      "fields": [
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSizeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralTokenDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "price",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceSlippage",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeToken",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "referral",
          "type": {
            "option": "publicKey"
          },
          "index": false
        },
        {
          "name": "positionFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "fundingFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceImpactFeeUsd",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "InstantDecreasePositionEvent",
      "fields": [
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSizeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "desiredMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "hasProfit",
          "type": "bool",
          "index": false
        },
        {
          "name": "pnlDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "transferAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "transferToken",
          "type": "u64",
          "index": false
        },
        {
          "name": "price",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceSlippage",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "referral",
          "type": {
            "option": "publicKey"
          },
          "index": false
        },
        {
          "name": "positionFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "fundingFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "originalPositionCollateralUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionCollateralUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceImpactFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionOpenTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "positionPrice",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "DepositCollateralEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "depositAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "userTokenAccount",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "time",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "WithdrawCollateralEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "withdrawAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "userTokenAccount",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "custody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "previousCollateralAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "marginUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "time",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "BorrowFromCustodyEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeCustodyToken",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "marginUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "updateTime",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "RepayToCustodyEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeCustodyToken",
          "type": "u64",
          "index": false
        },
        {
          "name": "updateTime",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "LiquidateBorrowPositionEvent",
      "fields": [
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSizeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "collateralLockedInUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralLockedInLp",
          "type": "u64",
          "index": false
        },
        {
          "name": "remainingCollateralInLp",
          "type": "u64",
          "index": false
        },
        {
          "name": "custodyTokenPrice",
          "type": "u64",
          "index": false
        },
        {
          "name": "totalBorrowsInUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "liquidationFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "liquidationTime",
          "type": "i64",
          "index": false
        }
      ]
    }
  ],
  "errors": [
    {
      "code": 6000,
      "name": "MathOverflow",
      "msg": "Overflow in arithmetic operation"
    },
    {
      "code": 6001,
      "name": "UnsupportedOracle",
      "msg": "Unsupported price oracle"
    },
    {
      "code": 6002,
      "name": "InvalidOracleAccount",
      "msg": "Invalid oracle account"
    },
    {
      "code": 6003,
      "name": "StaleOraclePrice",
      "msg": "Stale oracle price"
    },
    {
      "code": 6004,
      "name": "InvalidOraclePrice",
      "msg": "Invalid oracle price"
    },
    {
      "code": 6005,
      "name": "InvalidEnvironment",
      "msg": "Instruction is not allowed in production"
    },
    {
      "code": 6006,
      "name": "InvalidCollateralAccount",
      "msg": "Invalid collateral account"
    },
    {
      "code": 6007,
      "name": "InvalidCollateralAmount",
      "msg": "Invalid collateral amount"
    },
    {
      "code": 6008,
      "name": "CollateralSlippage",
      "msg": "Collateral slippage"
    },
    {
      "code": 6009,
      "name": "InvalidPositionState",
      "msg": "Invalid position state"
    },
    {
      "code": 6010,
      "name": "InvalidPerpetualsConfig",
      "msg": "Invalid perpetuals config"
    },
    {
      "code": 6011,
      "name": "InvalidPoolConfig",
      "msg": "Invalid pool config"
    },
    {
      "code": 6012,
      "name": "InvalidInstruction",
      "msg": "Invalid instruction"
    },
    {
      "code": 6013,
      "name": "InvalidCustodyConfig",
      "msg": "Invalid custody config"
    },
    {
      "code": 6014,
      "name": "InvalidCustodyBalance",
      "msg": "Invalid custody balance"
    },
    {
      "code": 6015,
      "name": "InvalidArgument",
      "msg": "Invalid argument"
    },
    {
      "code": 6016,
      "name": "InvalidPositionRequest",
      "msg": "Invalid position request"
    },
    {
      "code": 6017,
      "name": "InvalidPositionRequestInputAta",
      "msg": "Invalid position request input ata"
    },
    {
      "code": 6018,
      "name": "InvalidMint",
      "msg": "Invalid mint"
    },
    {
      "code": 6019,
      "name": "InsufficientTokenAmount",
      "msg": "Insufficient token amount"
    },
    {
      "code": 6020,
      "name": "InsufficientAmountReturned",
      "msg": "Insufficient token amount returned"
    },
    {
      "code": 6021,
      "name": "MaxPriceSlippage",
      "msg": "Price slippage limit exceeded"
    },
    {
      "code": 6022,
      "name": "MaxLeverage",
      "msg": "Position leverage limit exceeded"
    },
    {
      "code": 6023,
      "name": "CustodyAmountLimit",
      "msg": "Custody amount limit exceeded"
    },
    {
      "code": 6024,
      "name": "PoolAmountLimit",
      "msg": "Pool amount limit exceeded"
    },
    {
      "code": 6025,
      "name": "PersonalPoolAmountLimit",
      "msg": "Personal pool amount limit exceeded"
    },
    {
      "code": 6026,
      "name": "UnsupportedToken",
      "msg": "Token is not supported"
    },
    {
      "code": 6027,
      "name": "InstructionNotAllowed",
      "msg": "Instruction is not allowed at this time"
    },
    {
      "code": 6028,
      "name": "JupiterProgramMismatch",
      "msg": "Jupiter Program ID mismatch"
    },
    {
      "code": 6029,
      "name": "ProgramMismatch",
      "msg": "Program ID mismatch"
    },
    {
      "code": 6030,
      "name": "AddressMismatch",
      "msg": "Address mismatch"
    },
    {
      "code": 6031,
      "name": "KeeperATAMissing",
      "msg": "Missing keeper ATA"
    },
    {
      "code": 6032,
      "name": "SwapAmountMismatch",
      "msg": "Swap amount mismatch"
    },
    {
      "code": 6033,
      "name": "CPINotAllowed",
      "msg": "CPI not allowed"
    },
    {
      "code": 6034,
      "name": "InvalidKeeper",
      "msg": "Invalid Keeper"
    },
    {
      "code": 6035,
      "name": "ExceedExecutionPeriod",
      "msg": "Exceed execution period"
    },
    {
      "code": 6036,
      "name": "InvalidRequestType",
      "msg": "Invalid Request Type"
    },
    {
      "code": 6037,
      "name": "InvalidTriggerPrice",
      "msg": "Invalid Trigger Price"
    },
    {
      "code": 6038,
      "name": "TriggerPriceSlippage",
      "msg": "Trigger Price Slippage"
    },
    {
      "code": 6039,
      "name": "MissingTriggerPrice",
      "msg": "Missing Trigger Price"
    },
    {
      "code": 6040,
      "name": "MissingPriceSlippage",
      "msg": "Missing Price Slippage"
    },
    {
      "code": 6041,
      "name": "InvalidPriceCalcMode",
      "msg": "Invalid Price Calc Mode"
    },
    {
      "code": 6042,
      "name": "RequestUpdatedTooRecent",
      "msg": "Request Updated Too Recent"
    },
    {
      "code": 6043,
      "name": "ExceedTokenWeightage",
      "msg": "Exceed Token Weightage"
    },
    {
      "code": 6044,
      "name": "OraclePublishTimeTooEarly",
      "msg": "Oracle Publish Time Too Early"
    },
    {
      "code": 6045,
      "name": "PullOraclePublishTimeTooEarly",
      "msg": "Pull Oracle Publish Time Too Early"
    },
    {
      "code": 6046,
      "name": "StalePullOraclePrice",
      "msg": "Stale Pull Oracle Price"
    },
    {
      "code": 6047,
      "name": "InvalidPullOraclePrice",
      "msg": "Invalid Pull Oracle Price"
    },
    {
      "code": 6048,
      "name": "PullOracleNotVerified",
      "msg": "Pull Oracle Not Verified"
    },
    {
      "code": 6049,
      "name": "PriceDiffTooLarge",
      "msg": "Price Diff Between Pull and Push Oracle is Too Large"
    },
    {
      "code": 6050,
      "name": "InvalidDovesOraclePrice",
      "msg": "Invalid Doves Oracle Price"
    },
    {
      "code": 6051,
      "name": "InvalidRequestTime",
      "msg": "Invalid Request Time"
    },
    {
      "code": 6052,
      "name": "PositionUpdatedTooRecent",
      "msg": "Position Updated Too Recent"
    },
    {
      "code": 6053,
      "name": "LedgerTokenAccountDoesNotMatch",
      "msg": "Ledger token account does not match"
    },
    {
      "code": 6054,
      "name": "InvalidTokenLedger",
      "msg": "Invalid token ledger"
    },
    {
      "code": 6055,
      "name": "OraclePriceDifferenceTooLarge",
      "msg": "Oracle Price Difference Too Large"
    },
    {
      "code": 6056,
      "name": "InvalidOracleSigner",
      "msg": "Invalid Oracle Signer"
    },
    {
      "code": 6057,
      "name": "InvalidOracleTimestamp",
      "msg": "Invalid Oracle Timestamp"
    },
    {
      "code": 6058,
      "name": "InvalidMaxGlobalLongSize",
      "msg": "New Max Global Long Size Too Low"
    },
    {
      "code": 6059,
      "name": "InvalidMaxGlobalShortSize",
      "msg": "New Max Global Short Size Too Low"
    },
    {
      "code": 6060,
      "name": "BorrowsDisabled",
      "msg": "Borrows disabled for this custody"
    },
    {
      "code": 6061,
      "name": "BorrowLimitsExceeded",
      "msg": "Borrows limit exceeded for this custody"
    },
    {
      "code": 6062,
      "name": "WithdrawExceedsMarginLimits",
      "msg": "Withdraw exceeds margin of the custody"
    },
    {
      "code": 6063,
      "name": "CannotLiquidate",
      "msg": "Cannot liquidate"
    }
  ]
};

export const IDL: Perpetuals = {
  "version": "0.1.0",
  "name": "perpetuals",
  "instructions": [
    {
      "name": "init",
      "accounts": [
        {
          "name": "upgradeAuthority",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "admin",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetualsProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetualsProgramData",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InitParams"
          }
        }
      ]
    },
    {
      "name": "addPool",
      "accounts": [
        {
          "name": "admin",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "rent",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "AddPoolParams"
          }
        }
      ]
    },
    {
      "name": "addCustody",
      "accounts": [
        {
          "name": "admin",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyTokenMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "rent",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "AddCustodyParams"
          }
        }
      ]
    },
    {
      "name": "setCustodyConfig",
      "accounts": [
        {
          "name": "admin",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "SetCustodyConfigParams"
          }
        }
      ]
    },
    {
      "name": "setPoolConfig",
      "accounts": [
        {
          "name": "admin",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "SetPoolConfigParams"
          }
        }
      ]
    },
    {
      "name": "setPerpetualsConfig",
      "accounts": [
        {
          "name": "admin",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "SetPerpetualsConfigParams"
          }
        }
      ]
    },
    {
      "name": "transferAdmin",
      "accounts": [
        {
          "name": "admin",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "newAdmin",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "TransferAdminParams"
          }
        }
      ]
    },
    {
      "name": "withdrawFees2",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "receivingTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "WithdrawFees2Params"
          }
        }
      ]
    },
    {
      "name": "createTokenMetadata",
      "accounts": [
        {
          "name": "admin",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "metadata",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenMetadataProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "rent",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "CreateTokenMetadataParams"
          }
        }
      ]
    },
    {
      "name": "createTokenLedger",
      "accounts": [
        {
          "name": "tokenLedger",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "payer",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": []
    },
    {
      "name": "reallocCustody",
      "accounts": [
        {
          "name": "keeper",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "rent",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": []
    },
    {
      "name": "reallocPool",
      "accounts": [
        {
          "name": "keeper",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "rent",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": []
    },
    {
      "name": "operatorSetCustodyConfig",
      "accounts": [
        {
          "name": "operator",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "OperatorSetCustodyConfigParams"
          }
        }
      ]
    },
    {
      "name": "operatorSetPoolConfig",
      "accounts": [
        {
          "name": "operator",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "OperatorSetPoolConfigParams"
          }
        }
      ]
    },
    {
      "name": "testInit",
      "accounts": [
        {
          "name": "upgradeAuthority",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "admin",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "TestInitParams"
          }
        }
      ]
    },
    {
      "name": "setTestTime",
      "accounts": [
        {
          "name": "admin",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "SetTestTimeParams"
          }
        }
      ]
    },
    {
      "name": "setTokenLedger",
      "accounts": [
        {
          "name": "tokenLedger",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": []
    },
    {
      "name": "swap2",
      "accounts": [
        {
          "name": "owner",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "fundingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "receivingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "receivingCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "receivingCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "receivingCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "receivingCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "dispensingCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "Swap2Params"
          }
        }
      ]
    },
    {
      "name": "addLiquidity2",
      "accounts": [
        {
          "name": "owner",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "fundingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "AddLiquidity2Params"
          }
        }
      ]
    },
    {
      "name": "removeLiquidity2",
      "accounts": [
        {
          "name": "owner",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "receivingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "RemoveLiquidity2Params"
          }
        }
      ]
    },
    {
      "name": "createIncreasePositionMarketRequest",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "fundingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "inputMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "CreateIncreasePositionMarketRequestParams"
          }
        }
      ]
    },
    {
      "name": "createDecreasePositionRequest2",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "receivingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "desiredMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "CreateDecreasePositionRequest2Params"
          }
        }
      ]
    },
    {
      "name": "createDecreasePositionMarketRequest",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "receivingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "desiredMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "CreateDecreasePositionMarketRequestParams"
          }
        }
      ]
    },
    {
      "name": "updateDecreasePositionRequest2",
      "accounts": [
        {
          "name": "owner",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "UpdateDecreasePositionRequest2Params"
          }
        }
      ]
    },
    {
      "name": "closePositionRequest",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true,
          "isOptional": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "ownerAta",
          "isMut": true,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "ClosePositionRequestParams"
          }
        }
      ]
    },
    {
      "name": "closePositionRequest2",
      "accounts": [
        {
          "name": "keeper",
          "isMut": true,
          "isSigner": true,
          "isOptional": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "ownerAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "mint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": []
    },
    {
      "name": "increasePosition4",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "IncreasePosition4Params"
          }
        }
      ]
    },
    {
      "name": "increasePositionPreSwap",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "keeperAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "instruction",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "IncreasePositionPreSwapParams"
          }
        }
      ]
    },
    {
      "name": "increasePositionWithInternalSwap",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "receivingCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "receivingCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "receivingCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "receivingCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "IncreasePositionWithInternalSwapParams"
          }
        }
      ]
    },
    {
      "name": "decreasePosition4",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "DecreasePosition4Params"
          }
        }
      ]
    },
    {
      "name": "decreasePositionWithInternalSwap",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "dispensingCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "DecreasePositionWithInternalSwapParams"
          }
        }
      ]
    },
    {
      "name": "decreasePositionWithTpsl",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "DecreasePositionWithTpslParams"
          }
        }
      ]
    },
    {
      "name": "decreasePositionWithTpslAndInternalSwap",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "dispensingCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "dispensingCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "DecreasePositionWithTpslAndInternalSwapParams"
          }
        }
      ]
    },
    {
      "name": "liquidateFullPosition4",
      "accounts": [
        {
          "name": "signer",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "LiquidateFullPosition4Params"
          }
        }
      ]
    },
    {
      "name": "refreshAssetsUnderManagement",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "RefreshAssetsUnderManagementParams"
          }
        }
      ]
    },
    {
      "name": "setMaxGlobalSizes",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "SetMaxGlobalSizesParams"
          }
        }
      ]
    },
    {
      "name": "instantCreateTpsl",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "apiKeeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "receivingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "desiredMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InstantCreateTpslParams"
          }
        }
      ]
    },
    {
      "name": "instantCreateLimitOrder",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "apiKeeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "fundingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "positionRequestAta",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "inputMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InstantCreateLimitOrderParams"
          }
        }
      ]
    },
    {
      "name": "instantIncreasePosition",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "apiKeeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "fundingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenLedger",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InstantIncreasePositionParams"
          }
        }
      ]
    },
    {
      "name": "instantDecreasePosition",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "apiKeeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "receivingAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralCustodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "collateralCustodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "desiredMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "referral",
          "isMut": false,
          "isSigner": false,
          "isOptional": true
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "associatedTokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InstantDecreasePositionParams"
          }
        }
      ]
    },
    {
      "name": "instantUpdateLimitOrder",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "apiKeeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InstantUpdateLimitOrderParams"
          }
        }
      ]
    },
    {
      "name": "instantUpdateTpsl",
      "accounts": [
        {
          "name": "keeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "apiKeeper",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "owner",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "position",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "positionRequest",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "InstantUpdateTpslParams"
          }
        }
      ]
    },
    {
      "name": "getAddLiquidityAmountAndFee2",
      "accounts": [
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "GetAddLiquidityAmountAndFee2Params"
          }
        }
      ],
      "returns": {
        "defined": "AmountAndFee"
      }
    },
    {
      "name": "getRemoveLiquidityAmountAndFee2",
      "accounts": [
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyDovesPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custodyPythnetPriceAccount",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "GetRemoveLiquidityAmountAndFee2Params"
          }
        }
      ],
      "returns": {
        "defined": "AmountAndFee"
      }
    },
    {
      "name": "getAssetsUnderManagement2",
      "accounts": [
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "GetAssetsUnderManagement2Params"
          }
        }
      ],
      "returns": "u128"
    },
    {
      "name": "borrowFromCustody",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "borrowPosition",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "userTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "BorrowFromCustodyParams"
          }
        }
      ]
    },
    {
      "name": "repayToCustody",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "borrowPosition",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "custodyTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "userTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "RepayToCustodyParams"
          }
        }
      ]
    },
    {
      "name": "depositCollateralForBorrows",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "borrowPosition",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "userTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "DepositParams"
          }
        }
      ]
    },
    {
      "name": "withdrawCollateralForBorrows",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "borrowPosition",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "userTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "WithdrawParams"
          }
        }
      ]
    },
    {
      "name": "liquidateBorrowPosition",
      "accounts": [
        {
          "name": "signer",
          "isMut": false,
          "isSigner": true
        },
        {
          "name": "perpetuals",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "custody",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "transferAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "borrowPosition",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "collateralTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "lpTokenMint",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "LiquidateBorrowPositionParams"
          }
        }
      ]
    },
    {
      "name": "closeBorrowPosition",
      "accounts": [
        {
          "name": "owner",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "borrowPosition",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "eventAuthority",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "program",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "params",
          "type": {
            "defined": "CloseBorrowPositionParams"
          }
        }
      ]
    }
  ],
  "accounts": [
    {
      "name": "borrowPosition",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "owner",
            "type": "publicKey"
          },
          {
            "name": "pool",
            "type": "publicKey"
          },
          {
            "name": "custody",
            "type": "publicKey"
          },
          {
            "name": "openTime",
            "type": "i64"
          },
          {
            "name": "updateTime",
            "type": "i64"
          },
          {
            "name": "borrowSize",
            "type": "u128"
          },
          {
            "name": "cumulativeCompoundedInterestSnapshot",
            "type": "u128"
          },
          {
            "name": "lockedCollateral",
            "type": "u64"
          },
          {
            "name": "bump",
            "type": "u8"
          },
          {
            "name": "lastBorrowed",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "custody",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "pool",
            "type": "publicKey"
          },
          {
            "name": "mint",
            "type": "publicKey"
          },
          {
            "name": "tokenAccount",
            "type": "publicKey"
          },
          {
            "name": "decimals",
            "type": "u8"
          },
          {
            "name": "isStable",
            "type": "bool"
          },
          {
            "name": "oracle",
            "type": {
              "defined": "OracleParams"
            }
          },
          {
            "name": "pricing",
            "type": {
              "defined": "PricingParams"
            }
          },
          {
            "name": "permissions",
            "type": {
              "defined": "Permissions"
            }
          },
          {
            "name": "targetRatioBps",
            "type": "u64"
          },
          {
            "name": "assets",
            "type": {
              "defined": "Assets"
            }
          },
          {
            "name": "fundingRateState",
            "type": {
              "defined": "FundingRateState"
            }
          },
          {
            "name": "bump",
            "type": "u8"
          },
          {
            "name": "tokenAccountBump",
            "type": "u8"
          },
          {
            "name": "increasePositionBps",
            "type": "u64"
          },
          {
            "name": "decreasePositionBps",
            "type": "u64"
          },
          {
            "name": "maxPositionSizeUsd",
            "type": "u64"
          },
          {
            "name": "dovesOracle",
            "type": "publicKey"
          },
          {
            "name": "jumpRateState",
            "type": {
              "defined": "JumpRateState"
            }
          },
          {
            "name": "dovesAgOracle",
            "type": "publicKey"
          },
          {
            "name": "priceImpactBuffer",
            "type": {
              "defined": "PriceImpactBuffer"
            }
          },
          {
            "name": "borrowLendParameters",
            "type": {
              "defined": "BorrowLendParams"
            }
          },
          {
            "name": "borrowsFundingRateState",
            "type": {
              "defined": "FundingRateState"
            }
          },
          {
            "name": "debt",
            "type": "u128"
          },
          {
            "name": "borrowLendInterestsAccured",
            "type": "u128"
          },
          {
            "name": "borrowLimitInTokenAmount",
            "type": "u64"
          },
          {
            "name": "minInterestFeeBps",
            "type": "u64"
          },
          {
            "name": "minInterestFeeGracePeriodSeconds",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "perpetuals",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "permissions",
            "type": {
              "defined": "Permissions"
            }
          },
          {
            "name": "pools",
            "type": {
              "vec": "publicKey"
            }
          },
          {
            "name": "admin",
            "type": "publicKey"
          },
          {
            "name": "transferAuthorityBump",
            "type": "u8"
          },
          {
            "name": "perpetualsBump",
            "type": "u8"
          },
          {
            "name": "inceptionTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "pool",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "name",
            "type": "string"
          },
          {
            "name": "custodies",
            "type": {
              "vec": "publicKey"
            }
          },
          {
            "name": "aumUsd",
            "type": "u128"
          },
          {
            "name": "limit",
            "type": {
              "defined": "Limit"
            }
          },
          {
            "name": "fees",
            "type": {
              "defined": "Fees"
            }
          },
          {
            "name": "poolApr",
            "type": {
              "defined": "PoolApr"
            }
          },
          {
            "name": "maxRequestExecutionSec",
            "type": "i64"
          },
          {
            "name": "bump",
            "type": "u8"
          },
          {
            "name": "lpTokenBump",
            "type": "u8"
          },
          {
            "name": "inceptionTime",
            "type": "i64"
          },
          {
            "name": "parameterUpdateOracle",
            "type": {
              "defined": "Secp256k1Pubkey"
            }
          },
          {
            "name": "aumUsdUpdatedAt",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "positionRequest",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "owner",
            "type": "publicKey"
          },
          {
            "name": "pool",
            "type": "publicKey"
          },
          {
            "name": "custody",
            "type": "publicKey"
          },
          {
            "name": "position",
            "type": "publicKey"
          },
          {
            "name": "mint",
            "type": "publicKey"
          },
          {
            "name": "openTime",
            "type": "i64"
          },
          {
            "name": "updateTime",
            "type": "i64"
          },
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "collateralDelta",
            "type": "u64"
          },
          {
            "name": "requestChange",
            "type": {
              "defined": "RequestChange"
            }
          },
          {
            "name": "requestType",
            "type": {
              "defined": "RequestType"
            }
          },
          {
            "name": "side",
            "type": {
              "defined": "Side"
            }
          },
          {
            "name": "priceSlippage",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "jupiterMinimumOut",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "preSwapAmount",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "triggerPrice",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "triggerAboveThreshold",
            "type": {
              "option": "bool"
            }
          },
          {
            "name": "entirePosition",
            "type": {
              "option": "bool"
            }
          },
          {
            "name": "executed",
            "type": "bool"
          },
          {
            "name": "counter",
            "type": "u64"
          },
          {
            "name": "bump",
            "type": "u8"
          },
          {
            "name": "referral",
            "type": {
              "option": "publicKey"
            }
          }
        ]
      }
    },
    {
      "name": "position",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "owner",
            "type": "publicKey"
          },
          {
            "name": "pool",
            "type": "publicKey"
          },
          {
            "name": "custody",
            "type": "publicKey"
          },
          {
            "name": "collateralCustody",
            "type": "publicKey"
          },
          {
            "name": "openTime",
            "type": "i64"
          },
          {
            "name": "updateTime",
            "type": "i64"
          },
          {
            "name": "side",
            "type": {
              "defined": "Side"
            }
          },
          {
            "name": "price",
            "type": "u64"
          },
          {
            "name": "sizeUsd",
            "type": "u64"
          },
          {
            "name": "collateralUsd",
            "type": "u64"
          },
          {
            "name": "realisedPnlUsd",
            "type": "i64"
          },
          {
            "name": "cumulativeInterestSnapshot",
            "type": "u128"
          },
          {
            "name": "lockedAmount",
            "type": "u64"
          },
          {
            "name": "bump",
            "type": "u8"
          }
        ]
      }
    },
    {
      "name": "tokenLedger",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "tokenAccount",
            "type": "publicKey"
          },
          {
            "name": "amount",
            "type": "u64"
          }
        ]
      }
    }
  ],
  "types": [
    {
      "name": "AddCustodyParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "isStable",
            "type": "bool"
          },
          {
            "name": "oracle",
            "type": {
              "defined": "OracleParams"
            }
          },
          {
            "name": "pricing",
            "type": {
              "defined": "PricingParams"
            }
          },
          {
            "name": "permissions",
            "type": {
              "defined": "Permissions"
            }
          },
          {
            "name": "hourlyFundingDbps",
            "type": "u64"
          },
          {
            "name": "targetRatioBps",
            "type": "u64"
          },
          {
            "name": "increasePositionBps",
            "type": "u64"
          },
          {
            "name": "decreasePositionBps",
            "type": "u64"
          },
          {
            "name": "dovesOracle",
            "type": "publicKey"
          },
          {
            "name": "maxPositionSizeUsd",
            "type": "u64"
          },
          {
            "name": "jumpRate",
            "type": {
              "defined": "JumpRateState"
            }
          },
          {
            "name": "priceImpactFeeFactor",
            "type": "u64"
          },
          {
            "name": "priceImpactExponent",
            "type": "f32"
          },
          {
            "name": "deltaImbalanceThresholdDecimal",
            "type": "u64"
          },
          {
            "name": "maxFeeBps",
            "type": "u64"
          },
          {
            "name": "dovesAgOracle",
            "type": "publicKey"
          }
        ]
      }
    },
    {
      "name": "AddLiquidity2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "tokenAmountIn",
            "type": "u64"
          },
          {
            "name": "minLpAmountOut",
            "type": "u64"
          },
          {
            "name": "tokenAmountPreSwap",
            "type": {
              "option": "u64"
            }
          }
        ]
      }
    },
    {
      "name": "AddPoolParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "name",
            "type": "string"
          },
          {
            "name": "limit",
            "type": {
              "defined": "Limit"
            }
          },
          {
            "name": "fees",
            "type": {
              "defined": "Fees"
            }
          },
          {
            "name": "maxRequestExecutionSec",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "BorrowFromCustodyParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "amount",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "CloseBorrowPositionParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "ClosePositionRequestParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "CreateDecreasePositionMarketRequestParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "collateralUsdDelta",
            "type": "u64"
          },
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "priceSlippage",
            "type": "u64"
          },
          {
            "name": "jupiterMinimumOut",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "entirePosition",
            "type": {
              "option": "bool"
            }
          },
          {
            "name": "counter",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "CreateDecreasePositionRequest2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "collateralUsdDelta",
            "type": "u64"
          },
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "requestType",
            "type": {
              "defined": "RequestType"
            }
          },
          {
            "name": "priceSlippage",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "jupiterMinimumOut",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "triggerPrice",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "triggerAboveThreshold",
            "type": {
              "option": "bool"
            }
          },
          {
            "name": "entirePosition",
            "type": {
              "option": "bool"
            }
          },
          {
            "name": "counter",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "CreateIncreasePositionMarketRequestParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "collateralTokenDelta",
            "type": "u64"
          },
          {
            "name": "side",
            "type": {
              "defined": "Side"
            }
          },
          {
            "name": "priceSlippage",
            "type": "u64"
          },
          {
            "name": "jupiterMinimumOut",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "counter",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "CreateTokenMetadataParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "name",
            "type": "string"
          },
          {
            "name": "symbol",
            "type": "string"
          },
          {
            "name": "uri",
            "type": "string"
          }
        ]
      }
    },
    {
      "name": "DecreasePosition4Params",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "DecreasePositionWithInternalSwapParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "DecreasePositionWithTpslAndInternalSwapParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "DecreasePositionWithTpslParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "DepositParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "amount",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "GetAddLiquidityAmountAndFee2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "tokenAmountIn",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "GetAssetsUnderManagement2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "mode",
            "type": {
              "option": {
                "defined": "PriceCalcMode"
              }
            }
          }
        ]
      }
    },
    {
      "name": "GetRemoveLiquidityAmountAndFee2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "lpAmountIn",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "IncreasePosition4Params",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "IncreasePositionPreSwapParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "IncreasePositionWithInternalSwapParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "InitParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "allowSwap",
            "type": "bool"
          },
          {
            "name": "allowAddLiquidity",
            "type": "bool"
          },
          {
            "name": "allowRemoveLiquidity",
            "type": "bool"
          },
          {
            "name": "allowIncreasePosition",
            "type": "bool"
          },
          {
            "name": "allowDecreasePosition",
            "type": "bool"
          },
          {
            "name": "allowCollateralWithdrawal",
            "type": "bool"
          },
          {
            "name": "allowLiquidatePosition",
            "type": "bool"
          }
        ]
      }
    },
    {
      "name": "InstantCreateLimitOrderParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "collateralTokenDelta",
            "type": "u64"
          },
          {
            "name": "side",
            "type": {
              "defined": "Side"
            }
          },
          {
            "name": "triggerPrice",
            "type": "u64"
          },
          {
            "name": "triggerAboveThreshold",
            "type": "bool"
          },
          {
            "name": "counter",
            "type": "u64"
          },
          {
            "name": "requestTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "InstantCreateTpslParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "collateralUsdDelta",
            "type": "u64"
          },
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "triggerPrice",
            "type": "u64"
          },
          {
            "name": "triggerAboveThreshold",
            "type": "bool"
          },
          {
            "name": "entirePosition",
            "type": "bool"
          },
          {
            "name": "counter",
            "type": "u64"
          },
          {
            "name": "requestTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "InstantDecreasePositionParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "collateralUsdDelta",
            "type": "u64"
          },
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "priceSlippage",
            "type": "u64"
          },
          {
            "name": "entirePosition",
            "type": {
              "option": "bool"
            }
          },
          {
            "name": "requestTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "InstantIncreasePositionParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "collateralTokenDelta",
            "type": {
              "option": "u64"
            }
          },
          {
            "name": "side",
            "type": {
              "defined": "Side"
            }
          },
          {
            "name": "priceSlippage",
            "type": "u64"
          },
          {
            "name": "requestTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "InstantUpdateLimitOrderParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "triggerPrice",
            "type": "u64"
          },
          {
            "name": "requestTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "InstantUpdateTpslParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "triggerPrice",
            "type": "u64"
          },
          {
            "name": "requestTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "LiquidateBorrowPositionParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "LiquidateFullPosition4Params",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "OperatorSetCustodyConfigParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "pricing",
            "type": {
              "defined": "PricingParams"
            }
          },
          {
            "name": "hourlyFundingDbps",
            "type": "u64"
          },
          {
            "name": "targetRatioBps",
            "type": "u64"
          },
          {
            "name": "increasePositionBps",
            "type": "u64"
          },
          {
            "name": "decreasePositionBps",
            "type": "u64"
          },
          {
            "name": "maxPositionSizeUsd",
            "type": "u64"
          },
          {
            "name": "jumpRate",
            "type": {
              "defined": "JumpRateState"
            }
          },
          {
            "name": "priceImpactFeeFactor",
            "type": "u64"
          },
          {
            "name": "priceImpactExponent",
            "type": "f32"
          },
          {
            "name": "deltaImbalanceThresholdDecimal",
            "type": "u64"
          },
          {
            "name": "maxFeeBps",
            "type": "u64"
          },
          {
            "name": "borrowLendParameters",
            "type": {
              "defined": "BorrowLendParams"
            }
          },
          {
            "name": "borrowHourlyFundingDbps",
            "type": "u64"
          },
          {
            "name": "borrowLimitInTokenAmount",
            "type": "u64"
          },
          {
            "name": "minInterestFeeBps",
            "type": "u64"
          },
          {
            "name": "minInterestFeeGracePeriodSeconds",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "OperatorSetPoolConfigParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "fees",
            "type": {
              "defined": "Fees"
            }
          },
          {
            "name": "limit",
            "type": {
              "defined": "Limit"
            }
          },
          {
            "name": "maxRequestExecutionSec",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "RefreshAssetsUnderManagementParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "RemoveLiquidity2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "lpAmountIn",
            "type": "u64"
          },
          {
            "name": "minAmountOut",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "RepayToCustodyParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "amount",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "SetCustodyConfigParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "oracle",
            "type": {
              "defined": "OracleParams"
            }
          },
          {
            "name": "pricing",
            "type": {
              "defined": "PricingParams"
            }
          },
          {
            "name": "permissions",
            "type": {
              "defined": "Permissions"
            }
          },
          {
            "name": "hourlyFundingDbps",
            "type": "u64"
          },
          {
            "name": "targetRatioBps",
            "type": "u64"
          },
          {
            "name": "increasePositionBps",
            "type": "u64"
          },
          {
            "name": "decreasePositionBps",
            "type": "u64"
          },
          {
            "name": "dovesOracle",
            "type": "publicKey"
          },
          {
            "name": "maxPositionSizeUsd",
            "type": "u64"
          },
          {
            "name": "jumpRate",
            "type": {
              "defined": "JumpRateState"
            }
          },
          {
            "name": "priceImpactFeeFactor",
            "type": "u64"
          },
          {
            "name": "priceImpactExponent",
            "type": "f32"
          },
          {
            "name": "deltaImbalanceThresholdDecimal",
            "type": "u64"
          },
          {
            "name": "maxFeeBps",
            "type": "u64"
          },
          {
            "name": "dovesAgOracle",
            "type": "publicKey"
          },
          {
            "name": "borrowLendParameters",
            "type": {
              "defined": "BorrowLendParams"
            }
          },
          {
            "name": "borrowHourlyFundingDbps",
            "type": "u64"
          },
          {
            "name": "borrowLimitInTokenAmount",
            "type": "u64"
          },
          {
            "name": "minInterestFeeBps",
            "type": "u64"
          },
          {
            "name": "minInterestFeeGracePeriodSeconds",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "SetMaxGlobalSizesParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "maxGlobalLongSize",
            "type": "u64"
          },
          {
            "name": "maxGlobalShortSize",
            "type": "u64"
          },
          {
            "name": "recoveryId",
            "type": "u8"
          },
          {
            "name": "signature",
            "type": {
              "array": [
                "u8",
                64
              ]
            }
          },
          {
            "name": "referenceId",
            "type": {
              "array": [
                "u8",
                16
              ]
            }
          },
          {
            "name": "timestamp",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "SetPerpetualsConfigParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "permissions",
            "type": {
              "defined": "Permissions"
            }
          }
        ]
      }
    },
    {
      "name": "SetPoolConfigParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "fees",
            "type": {
              "defined": "Fees"
            }
          },
          {
            "name": "limit",
            "type": {
              "defined": "Limit"
            }
          },
          {
            "name": "maxRequestExecutionSec",
            "type": "i64"
          },
          {
            "name": "parameterUpdateOracle",
            "type": {
              "defined": "Secp256k1Pubkey"
            }
          }
        ]
      }
    },
    {
      "name": "SetTestTimeParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "time",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "Swap2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "amountIn",
            "type": "u64"
          },
          {
            "name": "minAmountOut",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "TestInitParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "allowSwap",
            "type": "bool"
          },
          {
            "name": "allowAddLiquidity",
            "type": "bool"
          },
          {
            "name": "allowRemoveLiquidity",
            "type": "bool"
          },
          {
            "name": "allowIncreasePosition",
            "type": "bool"
          },
          {
            "name": "allowDecreasePosition",
            "type": "bool"
          },
          {
            "name": "allowCollateralWithdrawal",
            "type": "bool"
          },
          {
            "name": "allowLiquidatePosition",
            "type": "bool"
          }
        ]
      }
    },
    {
      "name": "TransferAdminParams",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "UpdateDecreasePositionRequest2Params",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "sizeUsdDelta",
            "type": "u64"
          },
          {
            "name": "triggerPrice",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "WithdrawParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "amount",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "WithdrawFees2Params",
      "type": {
        "kind": "struct",
        "fields": []
      }
    },
    {
      "name": "PriceImpactBuffer",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "openInterest",
            "type": {
              "array": [
                "i64",
                60
              ]
            }
          },
          {
            "name": "lastUpdated",
            "type": "i64"
          },
          {
            "name": "feeFactor",
            "type": "u64"
          },
          {
            "name": "exponent",
            "type": "f32"
          },
          {
            "name": "deltaImbalanceThresholdDecimal",
            "type": "u64"
          },
          {
            "name": "maxFeeBps",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "Assets",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "feesReserves",
            "type": "u64"
          },
          {
            "name": "owned",
            "type": "u64"
          },
          {
            "name": "locked",
            "type": "u64"
          },
          {
            "name": "guaranteedUsd",
            "type": "u64"
          },
          {
            "name": "globalShortSizes",
            "type": "u64"
          },
          {
            "name": "globalShortAveragePrices",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "PricingParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "tradeImpactFeeScalar",
            "type": "u64"
          },
          {
            "name": "buffer",
            "type": "u64"
          },
          {
            "name": "swapSpread",
            "type": "u64"
          },
          {
            "name": "maxLeverage",
            "type": "u64"
          },
          {
            "name": "maxGlobalLongSizes",
            "type": "u64"
          },
          {
            "name": "maxGlobalShortSizes",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "FundingRateState",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "cumulativeInterestRate",
            "type": "u128"
          },
          {
            "name": "lastUpdate",
            "type": "i64"
          },
          {
            "name": "hourlyFundingDbps",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "JumpRateState",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "minRateBps",
            "type": "u64"
          },
          {
            "name": "maxRateBps",
            "type": "u64"
          },
          {
            "name": "targetRateBps",
            "type": "u64"
          },
          {
            "name": "targetUtilizationRate",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "BorrowLendParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "borrowsLimitInBps",
            "type": "u64"
          },
          {
            "name": "maintainanceMarginBps",
            "type": "u64"
          },
          {
            "name": "protocolFeeBps",
            "type": "u64"
          },
          {
            "name": "liquidationMargin",
            "type": "u64"
          },
          {
            "name": "liquidationFeeBps",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "OraclePrice",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "price",
            "type": "u64"
          },
          {
            "name": "exponent",
            "type": "i32"
          }
        ]
      }
    },
    {
      "name": "Price",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "price",
            "type": "u64"
          },
          {
            "name": "expo",
            "type": "i32"
          },
          {
            "name": "publishTime",
            "type": "i64"
          }
        ]
      }
    },
    {
      "name": "OracleParams",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "oracleAccount",
            "type": "publicKey"
          },
          {
            "name": "oracleType",
            "type": {
              "defined": "OracleType"
            }
          },
          {
            "name": "buffer",
            "type": "u64"
          },
          {
            "name": "maxPriceAgeSec",
            "type": "u32"
          }
        ]
      }
    },
    {
      "name": "AmountAndFee",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "amount",
            "type": "u64"
          },
          {
            "name": "fee",
            "type": "u64"
          },
          {
            "name": "feeBps",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "Permissions",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "allowSwap",
            "type": "bool"
          },
          {
            "name": "allowAddLiquidity",
            "type": "bool"
          },
          {
            "name": "allowRemoveLiquidity",
            "type": "bool"
          },
          {
            "name": "allowIncreasePosition",
            "type": "bool"
          },
          {
            "name": "allowDecreasePosition",
            "type": "bool"
          },
          {
            "name": "allowCollateralWithdrawal",
            "type": "bool"
          },
          {
            "name": "allowLiquidatePosition",
            "type": "bool"
          }
        ]
      }
    },
    {
      "name": "Fees",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "swapMultiplier",
            "type": "u64"
          },
          {
            "name": "stableSwapMultiplier",
            "type": "u64"
          },
          {
            "name": "addRemoveLiquidityBps",
            "type": "u64"
          },
          {
            "name": "swapBps",
            "type": "u64"
          },
          {
            "name": "taxBps",
            "type": "u64"
          },
          {
            "name": "stableSwapBps",
            "type": "u64"
          },
          {
            "name": "stableSwapTaxBps",
            "type": "u64"
          },
          {
            "name": "liquidationRewardBps",
            "type": "u64"
          },
          {
            "name": "protocolShareBps",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "PoolApr",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "lastUpdated",
            "type": "i64"
          },
          {
            "name": "feeAprBps",
            "type": "u64"
          },
          {
            "name": "realizedFeeUsd",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "Limit",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "maxAumUsd",
            "type": "u128"
          },
          {
            "name": "tokenWeightageBufferBps",
            "type": "u128"
          },
          {
            "name": "buffer",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "Secp256k1Pubkey",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "prefix",
            "type": "u8"
          },
          {
            "name": "key",
            "type": {
              "array": [
                "u8",
                32
              ]
            }
          }
        ]
      }
    },
    {
      "name": "PriceImpactMechanism",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "TradeSize"
          },
          {
            "name": "DeltaImbalance"
          }
        ]
      }
    },
    {
      "name": "OracleType",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "None"
          },
          {
            "name": "Test"
          },
          {
            "name": "Pyth"
          }
        ]
      }
    },
    {
      "name": "PriceCalcMode",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "Min"
          },
          {
            "name": "Max"
          },
          {
            "name": "Ignore"
          }
        ]
      }
    },
    {
      "name": "PriceStaleTolerance",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "Strict"
          },
          {
            "name": "Loose"
          }
        ]
      }
    },
    {
      "name": "TradePoolType",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "Increase"
          },
          {
            "name": "Decrease"
          }
        ]
      }
    },
    {
      "name": "RequestType",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "Market"
          },
          {
            "name": "Trigger"
          }
        ]
      }
    },
    {
      "name": "RequestChange",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "None"
          },
          {
            "name": "Increase"
          },
          {
            "name": "Decrease"
          }
        ]
      }
    },
    {
      "name": "Side",
      "type": {
        "kind": "enum",
        "variants": [
          {
            "name": "None"
          },
          {
            "name": "Long"
          },
          {
            "name": "Short"
          }
        ]
      }
    }
  ],
  "events": [
    {
      "name": "CreatePositionRequestEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceSlippage",
          "type": {
            "option": "u64"
          },
          "index": false
        },
        {
          "name": "jupiterMinimumOut",
          "type": {
            "option": "u64"
          },
          "index": false
        },
        {
          "name": "preSwapAmount",
          "type": {
            "option": "u64"
          },
          "index": false
        },
        {
          "name": "requestChange",
          "type": "u8",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "referral",
          "type": {
            "option": "publicKey"
          },
          "index": false
        }
      ]
    },
    {
      "name": "InstantCreateTpslEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "entirePosition",
          "type": "bool",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "InstantUpdateTpslEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "entirePosition",
          "type": "bool",
          "index": false
        },
        {
          "name": "updateTime",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "ClosePositionRequestEvent",
      "fields": [
        {
          "name": "entirePosition",
          "type": {
            "option": "bool"
          },
          "index": false
        },
        {
          "name": "executed",
          "type": "bool",
          "index": false
        },
        {
          "name": "requestChange",
          "type": "u8",
          "index": false
        },
        {
          "name": "requestType",
          "type": "u8",
          "index": false
        },
        {
          "name": "side",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "mint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "amount",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "IncreasePositionEvent",
      "fields": [
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSizeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestChange",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionRequestType",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionRequestCollateralDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralTokenDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "price",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceSlippage",
          "type": {
            "option": "u64"
          },
          "index": false
        },
        {
          "name": "feeToken",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "referral",
          "type": {
            "option": "publicKey"
          },
          "index": false
        },
        {
          "name": "positionFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "fundingFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceImpactFeeUsd",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "IncreasePositionPreSwapEvent",
      "fields": [
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "transferAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralCustodyPreSwapAmount",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "DecreasePositionEvent",
      "fields": [
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSizeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestChange",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionRequestType",
          "type": "u8",
          "index": false
        },
        {
          "name": "hasProfit",
          "type": "bool",
          "index": false
        },
        {
          "name": "pnlDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "transferAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "transferToken",
          "type": {
            "option": "u64"
          },
          "index": false
        },
        {
          "name": "price",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceSlippage",
          "type": {
            "option": "u64"
          },
          "index": false
        },
        {
          "name": "feeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "referral",
          "type": {
            "option": "publicKey"
          },
          "index": false
        },
        {
          "name": "positionFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "fundingFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceImpactFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "originalPositionCollateralUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionCollateralUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionOpenTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "positionPrice",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "DecreasePositionPostSwapEvent",
      "fields": [
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "swapAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "jupiterMinimumOut",
          "type": {
            "option": "u64"
          },
          "index": false
        }
      ]
    },
    {
      "name": "LiquidateFullPositionEvent",
      "fields": [
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSizeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "hasProfit",
          "type": "bool",
          "index": false
        },
        {
          "name": "pnlDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "transferAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "transferToken",
          "type": "u64",
          "index": false
        },
        {
          "name": "price",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "liquidationFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "positionFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "fundingFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceImpactFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "originalPositionCollateralUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionCollateralUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionOpenTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "positionPrice",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "PoolSwapEvent",
      "fields": [
        {
          "name": "receivingCustodyKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "dispensingCustodyKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "poolKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "amountIn",
          "type": "u64",
          "index": false
        },
        {
          "name": "amountOut",
          "type": "u64",
          "index": false
        },
        {
          "name": "swapUsdAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "amountOutAfterFees",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeBps",
          "type": "u64",
          "index": false
        },
        {
          "name": "ownerKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "receivingAccountKey",
          "type": "publicKey",
          "index": false
        }
      ]
    },
    {
      "name": "PoolSwapExactOutEvent",
      "fields": [
        {
          "name": "receivingCustodyKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "dispensingCustodyKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "poolKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "amountIn",
          "type": "u64",
          "index": false
        },
        {
          "name": "amountInAfterFees",
          "type": "u64",
          "index": false
        },
        {
          "name": "amountOut",
          "type": "u64",
          "index": false
        },
        {
          "name": "swapUsdAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeBps",
          "type": "u64",
          "index": false
        },
        {
          "name": "ownerKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "receivingAccountKey",
          "type": "publicKey",
          "index": false
        }
      ]
    },
    {
      "name": "AddLiquidityEvent",
      "fields": [
        {
          "name": "custodyKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "poolKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "tokenAmountIn",
          "type": "u64",
          "index": false
        },
        {
          "name": "prePoolAmountUsd",
          "type": "u128",
          "index": false
        },
        {
          "name": "tokenAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeBps",
          "type": "u64",
          "index": false
        },
        {
          "name": "tokenAmountAfterFee",
          "type": "u64",
          "index": false
        },
        {
          "name": "mintAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "lpAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "postPoolAmountUsd",
          "type": "u128",
          "index": false
        }
      ]
    },
    {
      "name": "RemoveLiquidityEvent",
      "fields": [
        {
          "name": "custodyKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "poolKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "lpAmountIn",
          "type": "u64",
          "index": false
        },
        {
          "name": "removeAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeBps",
          "type": "u64",
          "index": false
        },
        {
          "name": "removeTokenAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "tokenAmountAfterFee",
          "type": "u64",
          "index": false
        },
        {
          "name": "postPoolAmountUsd",
          "type": "u128",
          "index": false
        }
      ]
    },
    {
      "name": "InstantCreateLimitOrderEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionRequestMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "InstantIncreasePositionEvent",
      "fields": [
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSizeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralTokenDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "price",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceSlippage",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeToken",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "referral",
          "type": {
            "option": "publicKey"
          },
          "index": false
        },
        {
          "name": "positionFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "fundingFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceImpactFeeUsd",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "InstantDecreasePositionEvent",
      "fields": [
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSide",
          "type": "u8",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCollateralCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSizeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "desiredMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "hasProfit",
          "type": "bool",
          "index": false
        },
        {
          "name": "pnlDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeUsdDelta",
          "type": "u64",
          "index": false
        },
        {
          "name": "transferAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "transferToken",
          "type": "u64",
          "index": false
        },
        {
          "name": "price",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceSlippage",
          "type": "u64",
          "index": false
        },
        {
          "name": "feeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "openTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "referral",
          "type": {
            "option": "publicKey"
          },
          "index": false
        },
        {
          "name": "positionFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "fundingFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "originalPositionCollateralUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionCollateralUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "priceImpactFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "positionOpenTime",
          "type": "i64",
          "index": false
        },
        {
          "name": "positionPrice",
          "type": "u64",
          "index": false
        }
      ]
    },
    {
      "name": "DepositCollateralEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "depositAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "userTokenAccount",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "time",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "WithdrawCollateralEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "withdrawAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "userTokenAccount",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "custody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "previousCollateralAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "marginUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "time",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "BorrowFromCustodyEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeCustodyToken",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralAmount",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralAmountUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "marginUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "updateTime",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "RepayToCustodyEvent",
      "fields": [
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionMint",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "sizeCustodyToken",
          "type": "u64",
          "index": false
        },
        {
          "name": "updateTime",
          "type": "i64",
          "index": false
        }
      ]
    },
    {
      "name": "LiquidateBorrowPositionEvent",
      "fields": [
        {
          "name": "positionKey",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionCustody",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "positionSizeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "owner",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "pool",
          "type": "publicKey",
          "index": false
        },
        {
          "name": "collateralLockedInUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "collateralLockedInLp",
          "type": "u64",
          "index": false
        },
        {
          "name": "remainingCollateralInLp",
          "type": "u64",
          "index": false
        },
        {
          "name": "custodyTokenPrice",
          "type": "u64",
          "index": false
        },
        {
          "name": "totalBorrowsInUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "liquidationFeeUsd",
          "type": "u64",
          "index": false
        },
        {
          "name": "liquidationTime",
          "type": "i64",
          "index": false
        }
      ]
    }
  ],
  "errors": [
    {
      "code": 6000,
      "name": "MathOverflow",
      "msg": "Overflow in arithmetic operation"
    },
    {
      "code": 6001,
      "name": "UnsupportedOracle",
      "msg": "Unsupported price oracle"
    },
    {
      "code": 6002,
      "name": "InvalidOracleAccount",
      "msg": "Invalid oracle account"
    },
    {
      "code": 6003,
      "name": "StaleOraclePrice",
      "msg": "Stale oracle price"
    },
    {
      "code": 6004,
      "name": "InvalidOraclePrice",
      "msg": "Invalid oracle price"
    },
    {
      "code": 6005,
      "name": "InvalidEnvironment",
      "msg": "Instruction is not allowed in production"
    },
    {
      "code": 6006,
      "name": "InvalidCollateralAccount",
      "msg": "Invalid collateral account"
    },
    {
      "code": 6007,
      "name": "InvalidCollateralAmount",
      "msg": "Invalid collateral amount"
    },
    {
      "code": 6008,
      "name": "CollateralSlippage",
      "msg": "Collateral slippage"
    },
    {
      "code": 6009,
      "name": "InvalidPositionState",
      "msg": "Invalid position state"
    },
    {
      "code": 6010,
      "name": "InvalidPerpetualsConfig",
      "msg": "Invalid perpetuals config"
    },
    {
      "code": 6011,
      "name": "InvalidPoolConfig",
      "msg": "Invalid pool config"
    },
    {
      "code": 6012,
      "name": "InvalidInstruction",
      "msg": "Invalid instruction"
    },
    {
      "code": 6013,
      "name": "InvalidCustodyConfig",
      "msg": "Invalid custody config"
    },
    {
      "code": 6014,
      "name": "InvalidCustodyBalance",
      "msg": "Invalid custody balance"
    },
    {
      "code": 6015,
      "name": "InvalidArgument",
      "msg": "Invalid argument"
    },
    {
      "code": 6016,
      "name": "InvalidPositionRequest",
      "msg": "Invalid position request"
    },
    {
      "code": 6017,
      "name": "InvalidPositionRequestInputAta",
      "msg": "Invalid position request input ata"
    },
    {
      "code": 6018,
      "name": "InvalidMint",
      "msg": "Invalid mint"
    },
    {
      "code": 6019,
      "name": "InsufficientTokenAmount",
      "msg": "Insufficient token amount"
    },
    {
      "code": 6020,
      "name": "InsufficientAmountReturned",
      "msg": "Insufficient token amount returned"
    },
    {
      "code": 6021,
      "name": "MaxPriceSlippage",
      "msg": "Price slippage limit exceeded"
    },
    {
      "code": 6022,
      "name": "MaxLeverage",
      "msg": "Position leverage limit exceeded"
    },
    {
      "code": 6023,
      "name": "CustodyAmountLimit",
      "msg": "Custody amount limit exceeded"
    },
    {
      "code": 6024,
      "name": "PoolAmountLimit",
      "msg": "Pool amount limit exceeded"
    },
    {
      "code": 6025,
      "name": "PersonalPoolAmountLimit",
      "msg": "Personal pool amount limit exceeded"
    },
    {
      "code": 6026,
      "name": "UnsupportedToken",
      "msg": "Token is not supported"
    },
    {
      "code": 6027,
      "name": "InstructionNotAllowed",
      "msg": "Instruction is not allowed at this time"
    },
    {
      "code": 6028,
      "name": "JupiterProgramMismatch",
      "msg": "Jupiter Program ID mismatch"
    },
    {
      "code": 6029,
      "name": "ProgramMismatch",
      "msg": "Program ID mismatch"
    },
    {
      "code": 6030,
      "name": "AddressMismatch",
      "msg": "Address mismatch"
    },
    {
      "code": 6031,
      "name": "KeeperATAMissing",
      "msg": "Missing keeper ATA"
    },
    {
      "code": 6032,
      "name": "SwapAmountMismatch",
      "msg": "Swap amount mismatch"
    },
    {
      "code": 6033,
      "name": "CPINotAllowed",
      "msg": "CPI not allowed"
    },
    {
      "code": 6034,
      "name": "InvalidKeeper",
      "msg": "Invalid Keeper"
    },
    {
      "code": 6035,
      "name": "ExceedExecutionPeriod",
      "msg": "Exceed execution period"
    },
    {
      "code": 6036,
      "name": "InvalidRequestType",
      "msg": "Invalid Request Type"
    },
    {
      "code": 6037,
      "name": "InvalidTriggerPrice",
      "msg": "Invalid Trigger Price"
    },
    {
      "code": 6038,
      "name": "TriggerPriceSlippage",
      "msg": "Trigger Price Slippage"
    },
    {
      "code": 6039,
      "name": "MissingTriggerPrice",
      "msg": "Missing Trigger Price"
    },
    {
      "code": 6040,
      "name": "MissingPriceSlippage",
      "msg": "Missing Price Slippage"
    },
    {
      "code": 6041,
      "name": "InvalidPriceCalcMode",
      "msg": "Invalid Price Calc Mode"
    },
    {
      "code": 6042,
      "name": "RequestUpdatedTooRecent",
      "msg": "Request Updated Too Recent"
    },
    {
      "code": 6043,
      "name": "ExceedTokenWeightage",
      "msg": "Exceed Token Weightage"
    },
    {
      "code": 6044,
      "name": "OraclePublishTimeTooEarly",
      "msg": "Oracle Publish Time Too Early"
    },
    {
      "code": 6045,
      "name": "PullOraclePublishTimeTooEarly",
      "msg": "Pull Oracle Publish Time Too Early"
    },
    {
      "code": 6046,
      "name": "StalePullOraclePrice",
      "msg": "Stale Pull Oracle Price"
    },
    {
      "code": 6047,
      "name": "InvalidPullOraclePrice",
      "msg": "Invalid Pull Oracle Price"
    },
    {
      "code": 6048,
      "name": "PullOracleNotVerified",
      "msg": "Pull Oracle Not Verified"
    },
    {
      "code": 6049,
      "name": "PriceDiffTooLarge",
      "msg": "Price Diff Between Pull and Push Oracle is Too Large"
    },
    {
      "code": 6050,
      "name": "InvalidDovesOraclePrice",
      "msg": "Invalid Doves Oracle Price"
    },
    {
      "code": 6051,
      "name": "InvalidRequestTime",
      "msg": "Invalid Request Time"
    },
    {
      "code": 6052,
      "name": "PositionUpdatedTooRecent",
      "msg": "Position Updated Too Recent"
    },
    {
      "code": 6053,
      "name": "LedgerTokenAccountDoesNotMatch",
      "msg": "Ledger token account does not match"
    },
    {
      "code": 6054,
      "name": "InvalidTokenLedger",
      "msg": "Invalid token ledger"
    },
    {
      "code": 6055,
      "name": "OraclePriceDifferenceTooLarge",
      "msg": "Oracle Price Difference Too Large"
    },
    {
      "code": 6056,
      "name": "InvalidOracleSigner",
      "msg": "Invalid Oracle Signer"
    },
    {
      "code": 6057,
      "name": "InvalidOracleTimestamp",
      "msg": "Invalid Oracle Timestamp"
    },
    {
      "code": 6058,
      "name": "InvalidMaxGlobalLongSize",
      "msg": "New Max Global Long Size Too Low"
    },
    {
      "code": 6059,
      "name": "InvalidMaxGlobalShortSize",
      "msg": "New Max Global Short Size Too Low"
    },
    {
      "code": 6060,
      "name": "BorrowsDisabled",
      "msg": "Borrows disabled for this custody"
    },
    {
      "code": 6061,
      "name": "BorrowLimitsExceeded",
      "msg": "Borrows limit exceeded for this custody"
    },
    {
      "code": 6062,
      "name": "WithdrawExceedsMarginLimits",
      "msg": "Withdraw exceeds margin of the custody"
    },
    {
      "code": 6063,
      "name": "CannotLiquidate",
      "msg": "Cannot liquidate"
    }
  ]
};
