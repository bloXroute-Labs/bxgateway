from bxcommon.messages.eth.serializers.transaction import Transaction
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import (
    TransactionsEthProtocolMessage,
)
import bxcommon.test_utils.fixture.eth_fixtures as common_eth_fixtures
import blxr_rlp as rlp

# Ethereum transactions message received from an OpenEthereum node, that seems to use a slightly
# different RLP encoding pattern for the ACL transaction.
# Should contain 56 transactions, at which index 28 is the ACL transaction.
OPEN_ETH_BERLIN_TXS_MESSAGE = bytes.fromhex(
    "f92963f86f825c5985cf440e0600832dc6c094488552076d0e2fa44c9b241163b8885809c0cdba880e6ed27d666800008025a0b3d16fd8cda60184c70f3d3e8f15496c83b1c4d8db0656a10ff494236e9637faa0668aa27953a2e9d54d443894a0da397d7e3409b0df45c3e5b690132b2f60dd68f8aa808592ccba810083012b8494dac17f958d2ee523a2206206994597c13d831ec780b844095ea7b30000000000000000000000002faf487a4414fe77e2327f0bf4ae2a264a776ad2ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff25a0981220858a0fc8910c98bccbe41f2a14956e7831983d323eab07d0db8b9a2366a07426d375e5bba7a88a22e2045c3ec89807ec48d141ece1efbad7e5416a07a941f8aa808592ccba810083012b8494dac17f958d2ee523a2206206994597c13d831ec780b844095ea7b30000000000000000000000002faf487a4414fe77e2327f0bf4ae2a264a776ad2ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff25a0c1017ad6f94e0e3063f85a38d135121bd0e7bb629655c966a244a27c82298d24a06571f9c5f6872e480ada00950b8379f5923600919585901e2b38f9d06396eaa8f8ad830db876857e6d52760083032918944f9254c83eb525f9fcf346490bbb3ed28a81c66780b844a9059cbb000000000000000000000000ef1dde45b8c586673a9da5c3521a80377f8747f4000000000000000000000000000000000000000000004404b877571427e0000026a08da1f2a0de2ff4497147169a2aa86c48ec145d239f8dad1408c3fdf5e6f3a11ea04f5033d7a5c34cd7c9c6487d6288ec5de68ad0f8fd2e0cb57e8e1717dfa23413f86d8291ee857594587a00828cc7942ee3c683f9c76120bbed3c0b6937d100d9a1b50e8740a886b37ed600801ca016773a5b266cbadf63af9fd3e3b6159df2467e3d2ba74e71998f23313a782a62a058254ae6d87a75b017b5fc4417bb849f3134c4f622edeca5f2efad8284158b60f86b0685721646a400825208941ced06878c9d031d962cd187bcc76f8462952e98873be8cf24d8f0008026a0498660644b581ed4972893f5519f36860e873876cc8e6002eb94663bd00c38dca00d9eb988a208e64c0d28500ff712699c33ab4778041432cb4de3a3ad82386cf1f86e82021385721646a40082520894964f80efe18ce20a895929e520ec69ce55c5ca6b88012e40cc7e1db4008025a0da02e2167d8362417fff2cd37093a17a5872316e11b03e2a3ab94c28daf79d36a004667b039798845cfcd7182190a95c7124251e39d7df241dde33da551c643e8af9016d8211348571ff07b680830242e1947a250d5630b4cf539739df2c5dacb4c659f2488d80b901048803dbee000000000000000000000000000000000000000000000489988379c9c4c1f0000000000000000000000000000000000000000000000000004afcbf7389fd86d100000000000000000000000000000000000000000000000000000000000000a000000000000000000000000049f93910c29e8c6473088a98d957cbfabce959eb00000000000000000000000000000000000000000000000000000000607e2c9b0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000000000000000000000000bbbbca6a901c926f240b89eacb641d8aec7aeafd25a073f981d1545ae6673fb15d3dcbdbea83da2f5ac5ab2c962d903b61069f6b0cefa07b7e581ae5b87f52ce9d6fe7322f87bfc883ff8e637c9ffb45537016cf8407def8ac82d70b8571daabda008303d09094dac17f958d2ee523a2206206994597c13d831ec780b844a9059cbb000000000000000000000000cd063369457c5372ee1c914b04d31079352e0fd60000000000000000000000000000000000000000000000000000000015f44fbd1ba0804014d514a699190bb1a0bbea224b4e371c9ccf8f043eb09a9cc38be30db818a0627326223cfa441d7b5edd6b1fb8c6ec3950f40e673d96b34b9555f6e443d7c6f86c068571daabda008252089452d72e11a1b00ec71029200a3dc8d3a4aeabf9598801b251ba2fcaa0008025a05bc6f9240db9fbdbed73b6d6f1d331fb06305983f200b64f5585e00d4d6288f0a0135d086ed2c0c94c78502dcd5832cf5919422fa0e6639726aafd5be5dfbb4cdcf86c018571daabda008252089456e7ef52d6ed3812ae14fa0696ee14c58f7f9473880364a3745f9540008026a02497cea2969b256f851d91d54b4ae0a7a19e189dcab1e74f63bf363691857f51a06b80c65549510aa76071ffb65d6847eacc978afb51726783a22afee0251253c1f86c0285721646a40082520894261e228a5dfc781bd8b57cb31ea4e866991a56648803666a33b1f880008026a0de946987b73628cf090d8ccd0691c9a587dc4d1885e9854f9458a8edf70effc9a00f3fa49a36a44a527b404f6e429a7431f883276852d9b812dbb840c66e1a6764f8aa048571daabda008303d09094c00e94cb662c3520282e6f5717214004a7f2688880b844a9059cbb00000000000000000000000050bbc463dd172851f99624da372ff2e8ae960c9e0000000000000000000000000000000000000000000000000279331609873c0026a04291d3003ed4f4ed8b6f48e9f5c4cd8e27a3d1a7c611f5cbf8953c1767347699a01b7e50bb113c9cace064e4bc2484d851845fbf5f50525cabe8009d1f3ec33261f86c018571daabda008252089450bbc463dd172851f99624da372ff2e8ae960c9e8801b251ba2fcaa0008026a00a2f2ca3ce4091feaad9c9c6758c7ef5658f6c34cbb830dfb6ab67fdf970f1f1a050739331748bee42756cffe9ca1168ad22b46d5a1e72c39261f8d8ff3e0f4cd9f86c018571daabda0082520894c2100d3939012a7bfe2b6a7ece42e8763f3eb33c8807f728f9997b74008026a0647cf6c2c682c526909365e6965140f35f699baf724e97233af8c240e6631c50a07f3eeb8391158b23d4b1b627abaa05709e98e9310f7037fc2dde08a200bac9f1f86c0285721646a400825208940e82d80c0d3aea72bd58a59b3df158b2a0e65fb2880fc29b1e2f8ed4008025a036a3eb8113dca360a6da27e0c0a174177a30c949ce434def5b152f36b4d60401a0096b0514217917697a24b04a4075a559c4023e3a353052505f8330ab7fdcba87f86c25856fc23ac00082520894cc01c7c080d9772111756a91fe80b628d7460c0188885aff399e86a0008025a0dfc2140b92aa5d4e3234c5608fede25a1ba7272100eef8361554235239c29634a0046ad06ca3ef485906f95483c3c6e5b435f394f09b41c8ef95d821823619cad3f8ac82015c8568cc0cf50083030d4094dac17f958d2ee523a2206206994597c13d831ec780b844a9059cbb000000000000000000000000a4637dba0129b74169217ec7fb573b912b73a53d0000000000000000000000000000000000000000000000000000000006e7bb101ba0e605e607faa1bfb1bd379d48c38d96dc9a136bc4f82543559d05988743737412a04c186001e677c6742e781cbcf8dee6874276a46ab1529508ad0d599655379bb4f901548201658566367066008303204b947a250d5630b4cf539739df2c5dacb4c659f2488d8815207a13d3c04a1db8e4fb3bdb41000000000000000000000000000000000000000000000008ac7230489e8000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000d50938c168cfa25d0c65e7e5b065439adb0524d600000000000000000000000000000000000000000000000000000000607e30870000000000000000000000000000000000000000000000000000000000000002000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000000000000000000000000e6f1966d04cfcb9cd1b1dc4e8256d8b501b11cba26a06f983abf9cbbe5c3c71a2e5f13c4b2732c5295387cfc025713121dfe5a484eeca079b1f566f9b500fc1408b04f7c44cafe3495f0c974fdae9e9b2713f28ce3a0ddf8ab81bb8564599a1bb383013880943301ee63fb29f863f2333bd4466acb46cd8323e680b844a9059cbb00000000000000000000000049845a255ba7fd0230264805cabf52813cc5a8f800000000000000000000000000000000000000001917620e333189ddb520cbbf26a087f10ec3e067943bbc78ff1d042c79952c3f23fb850f4d047256e0a8c09f0ee9a0614af2f95b45422f0ff321d5b92421e907ec8be7d0297fc0bd994f59af09646af90151808562f3f95a0083025c85947a250d5630b4cf539739df2c5dacb4c659f2488d8747c17611f1c4adb8e4fb3bdb410000000000000000000000000000000000000000001d4a5bf24653638f4000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000a18aef2a9c0802949758bbc917dd4d174fc8a40200000000000000000000000000000000000000000000000000000000607e2f0d0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000000000000000000000000e09fb60e8d6e7e1cebbe821bd5c3fc67a40f86bf26a0bb9cd2004ff9fe369a89dd7375c275d85f349ae2b9b7172a13c1c3f6aa536dbfa06d0a1263d4ea4eb5d11e3e43b35bd754a584b88bacdaaeb92e249554de7c3067f90151018562f3f95a0083023ffc947a250d5630b4cf539739df2c5dacb4c659f2488d8711c37937e08000b8e47ff36ab500000000000000000000000000000000000000000007c56ea2e3bfa595d3147f0000000000000000000000000000000000000000000000000000000000000080000000000000000000000000a18aef2a9c0802949758bbc917dd4d174fc8a40200000000000000000000000000000000000000000000000000000000607e2f240000000000000000000000000000000000000000000000000000000000000002000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000000000000000000000000e09fb60e8d6e7e1cebbe821bd5c3fc67a40f86bf26a0cb25f3d85771fddce76c23002412ceb125de1b17b49cd214c944e28936670809a01405e884c6b5921d7da61dd0ec9d3d8d85cdfb62d1b172cb46ed12849074396ef86b288562058e320082520894cd2b6b8ee9a8a59245e7e86f05100d4999acdcca883782dace9d9000008025a0a58d4a1d16e570fdde210450011860f0028d49357dc55e27023b495f90f5ccf89fb1a5234818cfc6134cb637304a594393294e85062fd095ad96a842b30e5c48f8a91285604c7b28e982c8069495ad61b0a150d79219dcf64e1e6cc01f0b64c4ce80b844095ea7b30000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488dffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff25a06c3c243ba290c296ad76e77c335f66ca18e893bc62300cd26e1fa59bcb6423cca06100c0f582337fe4c3f2366d4cbb5242a67486169a9392e067f42ac8973646e3f9015282495b855d551d3ec183046d25940000000000007f150bd6f54c40a34d7c3d5e9f5680b8ebc1b683cc02020100000000000070eb4ade34905000000d4a11d5eeaac28ec3f61d100daf4d40471f18520600c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000000000000000000000000000000000000000000000000000000007edf1d37000000000000000000000000000000000000000000000000000000007ed68ad90174c99f3f5331676f6aec2756e1f39b4fc029a83e0600dac17f958d2ee523a2206206994597c13d831ec70000000000000000000000000000000000000000018a7ec6143925d76d3d5a5d000000000000000000000000000000000000000001870dc6710dfd1242ca3cd41ca07cde258d7f14e693d22f34eede700c93d65043817e167825ea84b8cbb73079269fa4ba05601d46994b2e74b629f8a14d462bd56011e5bb542546a9e5fa509c67f86c6d855d21dba00082520894679c95c1d1e7ccc66fed32533aed99fe7cb26783880d2f13f7789f00008026a03ad13f5c949175d5229c80692bfca83bf941ee4aacbc1413866262e8429c8cfda07969d18191690d3d3a8266987c23f4a93ff48de552e03470bf35883c1152c8acf8ea77855b45055000830927c094c77aab3c6d7dab46248f3cc3033c856171878bd580b8847d533c1e000000000000000000000000adc4b357fcfbdd3021d9faace39b61893b7c504a000000000000000000000000e2f3e5fdb3b13303f5c0c1ffe9998892059a424f000000000000000000000000000000000000000000000006a2a196cc90ddca000000000000000000000000000000000000000000000000000000000060cd454926a081e7bf2e5a84698e40883e2ca92ec870ac82bd698c679882b95313e2120cb2c3a018465abe05fc9343ac0c222cf5957965262b64997566781947fbb55b2db8a7d5f9018b7f855a687bcbe9830386b3947a250d5630b4cf539739df2c5dacb4c659f2488d80b9012438ed1739000000000000000000000000000000000000000000000009eb7205ba79eec602000000000000000000000000000000000000000000000000000000011b77ab3000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000004ff4f5c5d61dcd8725778a55d4f0699d4078b7ef00000000000000000000000000000000000000000000000000000000607e30870000000000000000000000000000000000000000000000000000000000000003000000000000000000000000bc4171f45ef0ef66e76f979df021a34b46dcc81d000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec726a0bed6592676221e3f7b222b6223b59d18a45687538ebddf009ab83f25d74e1e29a04960f91baaccd7af2361b3ab55876a01a27aec9e0e61c4df5726bce7099c51a901f902430183015733855879c3d8008302a52294e010fcda8894c16a8acfef7b37741a760faeddc480b8a48201aa3f000000000000000000000000514910771af9ca656af840dff83e8264ecf986ca00000000000000000000000000000000000000000000000ec9f53752c69b0000000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc20000000000000000000000000000000000000000000000003f1bfaa795bff200fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff90132f87a94514910771af9ca656af840dff83e8264ecf986caf863a092f8f282ec6b99ca6c4ad92a03ae6e0d1f3f96aa1686e2c1eb242652be96f9a0a09cc9f4f94495f92e87cff50b7e628f34e1efb04d830b7078524e232622673097a0efe1d2751a5394464b1ed45d4cb21402ad6a82c26dd19b30306986f47f7f7159f85994c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2f842a063e06fc1e9c0dbc49e259f4fee3ef140799a5e8ffc5a8b9890bd1122e00d4984a07bc0e0e4ea7b0fa1f8da6617b11ff8df7f37e8998e44cb6136cd9f66b2ba8e41f85994e010fcda8894c16a8acfef7b37741a760faeddc4f842a0c8b0053af66e34563eef51d9e903539c0b7f677d87f2259d706bf5e6ced8dafda0d0bcf4df132c65dad73803c5e5e1c826f151a3342680034a8a4c8e5f8eb0c13f01a05f0bf40bd4d446f76540860ca507cbd6b32fbebdefad88d21bcc314e4fabe826a0192a8713f90a912fba66395408f8f61cb09071e059f6a931dc4edb828ae7e2ebf9018b2c8558028e44008302f840947a250d5630b4cf539739df2c5dacb4c659f2488d80b9012438ed173900000000000000000000000000000000000000000000010f0cf064dd59200000000000000000000000000000000000000000000000000000000000033f0a272000000000000000000000000000000000000000000000000000000000000000a000000000000000000000000068b25867e78c39ad8b88a0d040ad47025dd42c6200000000000000000000000000000000000000000000000000000000607e2d030000000000000000000000000000000000000000000000000000000000000003000000000000000000000000c52c326331e9ce41f04484d3b5e5648158028804000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec726a05f78bd7e2a6e1dd60883f963d6de74e2ee4156d36ec3d586985f5c3843daefe6a02afc05bcd7690404876d55e7ea4678e03367397346b1b1982834adbf685b82c5f9018c81de8556d88852008302f794947a250d5630b4cf539739df2c5dacb4c659f2488d80b9012438ed173900000000000000000000000000000000000000000000000000000007aef557390000000000000000000000000000000000000000000057e40f5881b4af40786600000000000000000000000000000000000000000000000000000000000000a00000000000000000000000002ea06e88fe8da4d9d11cf22fed0895fbe452150e00000000000000000000000000000000000000000000000000000000607e30870000000000000000000000000000000000000000000000000000000000000003000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc20000000000000000000000008762db106b2c2a0bccb3a80d1ed41273552616e825a03f605519501e21afb07d9da3d34ac2911cfc7299552ef81a52674ebf5ee272e1a03cc2f7f5b8cfc572675da9c5c92424fd33a475fa9f6ea240da6e0312266443d1f86f83017e20855714231c0082520894991de5eea87fe30aa4e4ea2abe7d88b3cff361768808c94c1f5077f0008025a081d41a576a2e4f32371106a1cf8fa0bddaac84184d88b1fde254a99f71e9a2c0a04f94152ede69a016f57830a3303799035b0df43e027e94f72646a0ba8d61635df86f83017e21855714231c008252089405f267385ad3e8feddbe2c8937d19bfac83281ef880888eb3dd8dd80008025a0815d7583ff9ca9886e0ffc8dc038e7b46f6d88039a4f1961ebd8c85bc3314b24a02eaf23d6e868b13367a4d4a3d252564858802cba943c70beffce99bb82ed6e91f86e83017e22855714231c0082520894e0f20c6669e93bee9b0eb7c3566e1f8509822c9687915034f4e1b8008025a0f6fb6a1c11234fe1ed049368267d6970a64fc5bed5fa333d99e62bb847d0fb62a07eb362b5c09c6dc45b87fb303020d854169d902e7274a786ec248c20ac4c3eeaf86f83017e23855714231c00825208941c8d8409cabbe97a764583dc6887aa09d86cd2b388013517524a0214008026a0313d4e2aea2c1b129b985b8dec72ff71e4bd86c76832f2db8c86ee1e75170daca05b5f84f79729c889d58b144171ec381b605fe34d127c4318254ee4a7b33555f0f86f83017e24855714231c00825208942ac1a3a1bf94ba2ddfa134ea77aa8956bff9e718880221300993cf90008025a0f698ee36b8d2f26a14804c3e82dea1f9f65e1e339b33b67ffde16cae421848d8a07a42194150c5b89fd2aec06940aa4060eba7826c58745b9314eb181e5f822546f8ad83017e25855714231c008301062594dac17f958d2ee523a2206206994597c13d831ec780b844a9059cbb00000000000000000000000057bfac82322eef1e69fd1ad6a985bbe3fa1590f50000000000000000000000000000000000000000000000000000000033d30f3026a065903b305e250aee21d56c33439236d7c39bbbe398c5fa9a576b5539a9ddda81a00225ab76ed26687c401a02f063b3967b0f35a267a9f2d13422c3169d3298cbfdf86f8301ad8485545732313082520894b2b2374165adc165ad61c775b4e95677e5429ae88801171e658c7ff800801ba091f95d038a5146deed50fb7249a049b376dce5f7df4728d304cda3dc93ddf62da02cfe0b14770b8987a29fbbab2063d38cc326bce20bc1a38b4638923b580a911ff8720a8551b93af600830a3a58941fd6adba9fee5c18338f134e31b4a323afa06ad48901236efcbcbb3400008446ebaf3c25a0de12f9caf6646905a1e7c4e0b01816daad39c9ad083db4bc1666343514d28a65a05a26fe1df908d862bf8ce7bb0edf508a24dc0f2a393d25501045ee37b74ae4def8aa0b854f29944800830110c29495ad61b0a150d79219dcf64e1e6cc01f0b64c4ce80b844095ea7b30000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488dffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff25a017f43a398227b94252e5165e713ca671d21fc467aff422ad76d013082f809cf2a077170654cd93e0f491a92d569244ccf421dab2a293bcd88d63e49aaf9ce26b38f86b80854bab82720082520894a8445e2f0cd21b168746adef7a3e2481ff1a7dcf870fd0af46386ec88025a0e3802ef7eec15e05ac8e576a3677186f145de164c4665e183c6cedfad2c339cfa00993fb97d3a8e22a9594f214bcc17723287b8edee650e5c503556fe1f2bbbbd8f8aa808548ce8dca45830249f094406ae253fb0aa898f9912fb192c1e6deb9623a0780b844a9059cbb000000000000000000000000167a9333bf582556f35bd4d16a7e80e191aa6476000000000000000000000000000000000000000000004c3ba39c5e411100000026a0108e86166fb95d861445edce0ccea587188a005db279da445c3909cf1ff69da2a00d1fbc93e3379e0b806ef94491330878f3518c030fae1e88b93656b68173d94ff9018b4a8548160383d583032c4c947a250d5630b4cf539739df2c5dacb4c659f2488d80b901248803dbee000000000000000000000000000000000000000000000000000002003a37f0000000000000000000000000000000000000000000000000000000000131d2c21800000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000b2fe744c3062f0a48967135f5bf9cce919a7163600000000000000000000000000000000000000000000000000000000607e30480000000000000000000000000000000000000000000000000000000000000003000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc200000000000000000000000006a01a4d579479dd5d884ebf61a31727a3d8d44225a0572ae3b48e3ef7fd5f873726c0eba54f59e4d41f7f437b9d595da7f12c3925efa03628ce7fcdb00e61eb0834626a742ced8344045e70a1721659763cd1d53bfaa3f9016d82022e8548160383d58302ca6f947a250d5630b4cf539739df2c5dacb4c659f2488d80b90104791ac9470000000000000000000000000000000000000000000000412fd490701fdd8b5a0000000000000000000000000000000000000000000000001e59dc1e537ec9e900000000000000000000000000000000000000000000000000000000000000a000000000000000000000000058c8ff99c94007674c19ced6c69aa202386fdcc600000000000000000000000000000000000000000000000000000000607e304800000000000000000000000000000000000000000000000000000000000000020000000000000000000000001ae1b650f31e758f4406ec4e495f43666d4af77a000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc226a010960db3547232e354e7ca5cf8de9db0cfec96822a7c77ede633a2d2290d3d25a025c25e3b7f479161d66c0667b7a814366afb1e5500d106149f568db7e8883724f9010c82084d8548160383d58301a1b194a0c68c638235ee32657e8f720a23cec1bfc77c7780b8a4e3dec8fb00000000000000000000000002aa0b826c7ba6386ddbe04c0a8715a1c0a16b2400000000000000000000000063b4f3e3fa4e438698ce330e365e831f7ccd1ef40000000000000000000000000000000000000000000000000000000000000060000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000381ee5a05995cc1ca26a0b711dff4e78196d0234ce9106b212b4ebbc1c4467c5889d9b8c8082cf165db8ea05a5caf3adbfbe62fdeea2b96e41dad666695479573df4f4b82c460da1820124ff8a93b8548160383d582c79f946b175474e89094c44da98b954eedeac495271d0f80b844095ea7b3000000000000000000000000d9e1ce17f2641f24ae83637ab66a2cca9c378b9fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff25a0b07b59fa6a8254b72458eed755cb56c0236a6b85841153b0350cf0122d401884a0606c478b641c6a804acc82a83a81e67cfa2de1ed6257e16f99812fd547756df3f9016b048548160383d58302381f947a250d5630b4cf539739df2c5dacb4c659f2488d80b901044a25d94a0000000000000000000000000000000000000000000000001bc16d674ec800000000000000000000000000000000000000000000040001e2f4f058280611ea5000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000003525d3e23ad93a0f0d5ebdf6dcc7cec159efa2c500000000000000000000000000000000000000000000000000000000607e3087000000000000000000000000000000000000000000000000000000000000000200000000000000000000000095ad61b0a150d79219dcf64e1e6cc01f0b64c4ce000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc226a0d4a823264abac206238220f1251ebaf3a80e7ddc9d53bc4cef958a508b7ba4a7a0336f7f28af03760deddfe14f05a55a673ac29e571cc0c4c9d22d5cb3fcd411edf8aa128533ebd5fb438301388094ebf5ffaed443093dbda54eed6dc77f7bba6dc7f280b844a9059cbb0000000000000000000000006eaf1adb169f58d17327686162b1827337014fc20000000000000000000000000000000000000000000000000000000ba43b740026a0c5f81d94d81918d9f58af5abc7336fc9069d4f413d786977a04f6294cecdfaeea02b2b7919f126f06803fe4181ab21effa708571b91c8ab14fea9fb3fc82aaa688f86b01852ecc889a0082520894a4e5961b58dbe487639929643dcb1dc3848daf5e8762e9324ef59c008025a06cf008b6ca9b2afd8732ece3425c7c0459f78c1e3be1fb3c7218c967b88c7793a076c88d702877fd9cbb64c77cc13595af13535f32db435881b35d91c6cccd4276f86b01852ecc889a0082520894a4e5961b58dbe487639929643dcb1dc3848daf5e87642a9a546068008026a079bc92d167bb89797bd7a3ce6aad6860f7a4b5be75449e1849cfb8e911346ffea024079eabb14fb42505974f781254afee7a7b53e7e7b0036e6a449bdd5ec7222bf86b01852ecc889a0082520894a4e5961b58dbe487639929643dcb1dc3848daf5e8762fd9d5b3c10008026a0f90f1bddb561be9cba4cd42df748366da0f7818d81e157d18473fae2ae059054a045ae540390ea2c7f5d5309dbaee2f09fae9268311c3da4de4876fdfc65ba005cf86b01852ecc889a0082520894a4e5961b58dbe487639929643dcb1dc3848daf5e8763a245041ff0008026a0ce49354fdb12c02171aa58f35fa5f659e296f38b8e4fd1cea667a61956d0d273a0692445925974d095202d8818ffdbd009401354d42b2120742a9e1c3e81518042f86b01852ecc889a0082520894a4e5961b58dbe487639929643dcb1dc3848daf5e8765f0652de324008025a0726815e1b08f0e0fcadf78892a0b9e6c88adb358469bd5793604a4b166cc434fa002a66ee5efe1324b2251c61070944ab77b0d69823104afe543c06864f8048032f86b01852ecc889a0082520894a4e5961b58dbe487639929643dcb1dc3848daf5e876516c0ce647c008026a09f6323cae3825545554431cd620c3f7348f4a956a1626b433d077d88661b3832a07d13b8a319c6f743b70660614f47d7479ee16edf172140b3ee1e7a6c9be5f731f86b01852ecc889a0082520894a4e5961b58dbe487639929643dcb1dc3848daf5e8735749106dea8008025a06d76ffaca10fa50a97407419e9b1551d897237ccfa5c21e9a571220300ac9d40a05c54882ab00ad1064ad06abe1bc90a05c1c5cff31cf57f50b12d6a0d12cf7929f86414852ad27c7c00825208941052cb62606c9e42a614abe2e3915cf05ad2b2fd808026a0134455598ed4fcca8784955bdf5a75fa6ee71a6b3eacaf9d331a300453b3e083a054aef15e9384eeee50442ab9cd8bb2f78b6d003d87b069101a8fa01a14532902"
)

EIP_155_TXS_MESSAGE = TransactionsEthProtocolMessage(
    None, [rlp.decode(common_eth_fixtures.TRANSACTION_EIP_155, Transaction)]
)
